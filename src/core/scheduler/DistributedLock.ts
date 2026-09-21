import { IStorageAdapter } from '../storage/IStorageAdapter.js';
import { ClusterBus } from '../ClusterBus.js';

export class DistributedLock {
  private renewTimer: NodeJS.Timeout | null = null;
  private isHeld = false;

  constructor(
    private storage: IStorageAdapter,
    private lockKey: string,
    private ownerId: string,
    private ttlMs: number = 5000
  ) {}

  public async acquire(): Promise<boolean> {
    const success = await this.storage.acquireLock(this.lockKey, this.ownerId, this.ttlMs);
    if (success) {
      this.isHeld = true;
      this.startHeartbeat();
    }
    return success;
  }

  private startHeartbeat(): void {
    if (this.renewTimer) clearInterval(this.renewTimer);
    // Renew at 40% of the TTL interval
    const intervalMs = Math.max(1000, Math.floor(this.ttlMs * 0.4));
    this.renewTimer = setInterval(async () => {
      if (!this.isHeld) {
        this.stopHeartbeat();
        return;
      }
      const renewed = await this.storage.renewLock(this.lockKey, this.ownerId, this.ttlMs);
      if (!renewed) {
        this.isHeld = false;
        this.stopHeartbeat();
        ClusterBus.getInstance().emit(
          'WARN',
          'DistributedLock',
          `Lock lease expired for ${this.lockKey} (owner: ${this.ownerId})`
        );
      }
    }, intervalMs);
  }

  private stopHeartbeat(): void {
    if (this.renewTimer) {
      clearInterval(this.renewTimer);
      this.renewTimer = null;
    }
  }

  public async release(): Promise<boolean> {
    this.isHeld = false;
    this.stopHeartbeat();
    return this.storage.releaseLock(this.lockKey, this.ownerId);
  }

  public isLockHeld(): boolean {
    return this.isHeld;
  }
}
