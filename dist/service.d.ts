import type { PageView, AnalyticsMetrics } from './types.js';
export declare class AnalyticsDataService {
    private lokiUrl;
    private prometheusUrl;
    private fetchLoki;
    private logger;
    constructor();
    getPageViews(timeRange?: string, limit?: number): Promise<PageView[]>;
    getTopPages(timeRange?: string, limit?: number): Promise<Array<{
        path: string;
        views: number;
    }>>;
    getUniqueVisitors(timeRange?: string): Promise<number>;
    getActiveUsers(): Promise<number>;
    getAverageSessionDuration(timeRange?: string): Promise<number>;
    getBounceRate(timeRange?: string): Promise<number>;
    getTrafficSources(timeRange?: string, limit?: number): Promise<Array<{
        source: string;
        visits: number;
    }>>;
    getAnalyticsMetrics(timeRange?: string): Promise<AnalyticsMetrics>;
}
export declare let analyticsDataService: AnalyticsDataService;
export declare function createAnalyticsDataService(): AnalyticsDataService;
//# sourceMappingURL=service.d.ts.map