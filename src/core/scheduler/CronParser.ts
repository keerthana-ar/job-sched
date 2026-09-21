import cronParser from 'cron-parser';

export class CronEngine {
  /**
   * Computes next execution timestamp in milliseconds from a cron expression.
   */
  public static getNextRunTime(cronExpression: string, fromDate: Date = new Date(), timezone: string = 'UTC'): number {
    try {
      const interval = cronParser.parseExpression(cronExpression, {
        currentDate: fromDate,
        tz: timezone,
      });
      return interval.next().getTime();
    } catch (err: any) {
      throw new Error(`Invalid cron expression '${cronExpression}': ${err.message}`);
    }
  }

  public static isValid(cronExpression: string): boolean {
    try {
      cronParser.parseExpression(cronExpression);
      return true;
    } catch {
      return false;
    }
  }
}
