import { ClusterLogEvent } from './types.js';

type Listener = (event: ClusterLogEvent) => void;

export class ClusterBus {
  private static instance: ClusterBus;
  private listeners: Set<Listener> = new Set();
  private recentLogs: ClusterLogEvent[] = [];
  private maxLogs = 200;

  private constructor() {}

  public static getInstance(): ClusterBus {
    if (!ClusterBus.instance) {
      ClusterBus.instance = new ClusterBus();
    }
    return ClusterBus.instance;
  }

  public subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  public emit(level: 'INFO' | 'WARN' | 'ERROR' | 'CHAOS', source: string, message: string, metadata?: Record<string, any>) {
    const event: ClusterLogEvent = {
      id: 'log_' + Math.random().toString(36).substring(2, 9),
      timestamp: Date.now(),
      level,
      source,
      message,
      metadata,
    };

    this.recentLogs.unshift(event);
    if (this.recentLogs.length > this.maxLogs) {
      this.recentLogs.pop();
    }

    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (err) {
        console.error('Error dispatching cluster event:', err);
      }
    }
  }

  public getRecentLogs(): ClusterLogEvent[] {
    return [...this.recentLogs];
  }
}
