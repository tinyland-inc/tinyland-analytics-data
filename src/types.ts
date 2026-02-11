/**
 * Represents a single page view event parsed from Loki log data.
 */
export interface PageView {
  timestamp: string;
  path: string;
  sessionId?: string;
  userId?: string;
  clientIp?: string;
  referrer?: string;
  userAgent?: string;
}

/**
 * Aggregated analytics metrics for a given time range.
 */
export interface AnalyticsMetrics {
  totalPageViews: number;
  uniqueVisitors: number;
  topPages: Array<{ path: string; views: number }>;
  averageSessionDuration: number;
  bounceRate: number;
}

/**
 * Minimal logger interface for dependency injection.
 * Compatible with pino, winston, console, or any structured logger.
 */
export interface Logger {
  info: (data: Record<string, unknown>, msg: string) => void;
  warn: (data: Record<string, unknown>, msg: string) => void;
  error: (data: Record<string, unknown>, msg: string) => void;
}

/**
 * Minimal fetch response interface for dependency injection.
 * Compatible with the native Response type but avoids coupling to it.
 */
export interface FetchResponse {
  ok: boolean;
  status: number;
  statusText: string;
  json: () => Promise<unknown>;
}

/**
 * Configuration required to initialize the analytics data package.
 * Must be provided via configure() before any service methods are called.
 */
export interface AnalyticsDataConfig {
  lokiUrl: string;
  prometheusUrl: string;
  fetchLoki: (path: string) => Promise<FetchResponse>;
  logger?: Logger;
}
