import type { AnalyticsDataConfig } from './types.js';

let config: AnalyticsDataConfig | null = null;

/**
 * Configure the analytics data package with required dependencies.
 * Must be called once before using any service methods.
 */
export function configure(c: AnalyticsDataConfig): void {
  config = c;
}

/**
 * Retrieve the current configuration.
 * Throws if configure() has not been called.
 */
export function getConfig(): AnalyticsDataConfig {
  if (!config) {
    throw new Error('tinyland-analytics-data: call configure() before use');
  }
  return config;
}

/**
 * Reset configuration to null. Useful for testing teardown.
 */
export function resetConfig(): void {
  config = null;
}
