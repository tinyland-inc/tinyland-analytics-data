import { getConfig } from './config.js';
import { parseTimeRange } from './time-utils.js';
const defaultLogger = {
    info: (data, msg) => console.log(msg, data),
    warn: (data, msg) => console.warn(msg, data),
    error: (data, msg) => console.error(msg, data),
};
export class AnalyticsDataService {
    lokiUrl;
    prometheusUrl;
    fetchLoki;
    logger;
    constructor() {
        const config = getConfig();
        this.lokiUrl = config.lokiUrl;
        this.prometheusUrl = config.prometheusUrl;
        this.fetchLoki = config.fetchLoki;
        this.logger = config.logger ?? defaultLogger;
    }
    async getPageViews(timeRange = '24h', limit = 1000) {
        try {
            const end = Date.now();
            const start = end - parseTimeRange(timeRange, this.logger);
            const query = `{job="stonewall-observability"} | json | component="analytics" | event_type="page_view"`;
            const path = `/loki/api/v1/query_range?query=${encodeURIComponent(query)}&start=${start}000000&end=${end}000000&limit=${limit}`;
            this.logger.info({ timeRange, limit, query }, 'Fetching page views from Loki');
            const response = await this.fetchLoki(path);
            if (!response.ok) {
                throw new Error(`Loki query failed: ${response.status} ${response.statusText}`);
            }
            const data = (await response.json());
            const pageViews = [];
            if (data.data?.result) {
                for (const stream of data.data.result) {
                    for (const [timestamp, logLine] of stream.values) {
                        try {
                            const parsed = JSON.parse(logLine);
                            pageViews.push({
                                timestamp: new Date(parseInt(timestamp) / 1000000).toISOString(),
                                path: parsed.path || '/',
                                sessionId: parsed.session_id,
                                userId: parsed.user_id,
                                clientIp: parsed.client_ip,
                                referrer: parsed.referrer,
                                userAgent: parsed.user_agent,
                            });
                        }
                        catch (err) {
                            this.logger.warn({ logLine, error: err }, 'Failed to parse log line');
                        }
                    }
                }
            }
            this.logger.info({ count: pageViews.length }, 'Fetched page views from Loki');
            return pageViews.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to fetch page views from Loki');
            return [];
        }
    }
    async getTopPages(timeRange = '24h', limit = 10) {
        try {
            const pageViews = await this.getPageViews(timeRange, 10000);
            const pathCounts = new Map();
            for (const view of pageViews) {
                const count = pathCounts.get(view.path) || 0;
                pathCounts.set(view.path, count + 1);
            }
            const topPages = Array.from(pathCounts.entries())
                .map(([path, views]) => ({ path, views }))
                .sort((a, b) => b.views - a.views)
                .slice(0, limit);
            return topPages;
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to get top pages');
            return [];
        }
    }
    async getUniqueVisitors(timeRange = '24h') {
        try {
            const pageViews = await this.getPageViews(timeRange, 10000);
            const uniqueVisitors = new Set();
            for (const view of pageViews) {
                const visitorId = view.sessionId || view.clientIp || 'unknown';
                if (visitorId !== 'unknown') {
                    uniqueVisitors.add(visitorId);
                }
            }
            return uniqueVisitors.size;
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to get unique visitors');
            return 0;
        }
    }
    async getActiveUsers() {
        try {
            const pageViews = await this.getPageViews('5m', 1000);
            const activeSessions = new Set();
            for (const view of pageViews) {
                if (view.sessionId) {
                    activeSessions.add(view.sessionId);
                }
            }
            return activeSessions.size;
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to get active users');
            return 0;
        }
    }
    async getAverageSessionDuration(timeRange = '24h') {
        try {
            const pageViews = await this.getPageViews(timeRange, 10000);
            const sessionMap = new Map();
            for (const view of pageViews) {
                if (!view.sessionId)
                    continue;
                const timestamp = new Date(view.timestamp).getTime();
                const session = sessionMap.get(view.sessionId);
                if (!session) {
                    sessionMap.set(view.sessionId, { first: timestamp, last: timestamp, count: 1 });
                }
                else {
                    session.first = Math.min(session.first, timestamp);
                    session.last = Math.max(session.last, timestamp);
                    session.count++;
                }
            }
            const sessions = Array.from(sessionMap.values());
            if (sessions.length === 0)
                return 0;
            const totalDuration = sessions.reduce((sum, session) => {
                return sum + (session.last - session.first);
            }, 0);
            return Math.round(totalDuration / sessions.length / 1000);
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to calculate average session duration');
            return 0;
        }
    }
    async getBounceRate(timeRange = '24h') {
        try {
            const pageViews = await this.getPageViews(timeRange, 10000);
            const sessionCounts = new Map();
            for (const view of pageViews) {
                if (!view.sessionId)
                    continue;
                const count = sessionCounts.get(view.sessionId) || 0;
                sessionCounts.set(view.sessionId, count + 1);
            }
            if (sessionCounts.size === 0)
                return 0;
            const bounceSessions = Array.from(sessionCounts.values()).filter((count) => count === 1).length;
            return Math.round((bounceSessions / sessionCounts.size) * 100);
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to calculate bounce rate');
            return 0;
        }
    }
    async getTrafficSources(timeRange = '24h', limit = 10) {
        try {
            const pageViews = await this.getPageViews(timeRange, 10000);
            const sourceCounts = new Map();
            for (const view of pageViews) {
                if (!view.referrer) {
                    const sessions = sourceCounts.get('Direct') || new Set();
                    if (view.sessionId)
                        sessions.add(view.sessionId);
                    sourceCounts.set('Direct', sessions);
                }
                else {
                    try {
                        const url = new URL(view.referrer);
                        const hostname = url.hostname;
                        let source = hostname;
                        if (hostname.includes('google'))
                            source = 'Google';
                        else if (hostname.includes('bing'))
                            source = 'Bing';
                        else if (hostname.includes('duckduckgo'))
                            source = 'DuckDuckGo';
                        else if (hostname.includes('twitter') || hostname === 't.co')
                            source = 'Twitter';
                        else if (hostname.includes('facebook'))
                            source = 'Facebook';
                        else if (hostname.includes('reddit'))
                            source = 'Reddit';
                        else if (hostname.includes('github'))
                            source = 'GitHub';
                        else
                            source = hostname;
                        const sessions = sourceCounts.get(source) || new Set();
                        if (view.sessionId)
                            sessions.add(view.sessionId);
                        sourceCounts.set(source, sessions);
                    }
                    catch {
                        const sessions = sourceCounts.get('Direct') || new Set();
                        if (view.sessionId)
                            sessions.add(view.sessionId);
                        sourceCounts.set('Direct', sessions);
                    }
                }
            }
            const trafficSources = Array.from(sourceCounts.entries())
                .map(([source, sessions]) => ({ source, visits: sessions.size }))
                .sort((a, b) => b.visits - a.visits)
                .slice(0, limit);
            return trafficSources;
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to get traffic sources');
            return [];
        }
    }
    async getAnalyticsMetrics(timeRange = '24h') {
        try {
            const [pageViews, topPages, uniqueVisitors, avgDuration, bounceRate] = await Promise.all([
                this.getPageViews(timeRange),
                this.getTopPages(timeRange, 10),
                this.getUniqueVisitors(timeRange),
                this.getAverageSessionDuration(timeRange),
                this.getBounceRate(timeRange),
            ]);
            return {
                totalPageViews: pageViews.length,
                uniqueVisitors,
                topPages,
                averageSessionDuration: avgDuration,
                bounceRate,
            };
        }
        catch (error) {
            this.logger.error({ error: error instanceof Error ? error.message : 'Unknown error' }, 'Failed to get analytics metrics');
            return {
                totalPageViews: 0,
                uniqueVisitors: 0,
                topPages: [],
                averageSessionDuration: 0,
                bounceRate: 0,
            };
        }
    }
}
export let analyticsDataService;
export function createAnalyticsDataService() {
    analyticsDataService = new AnalyticsDataService();
    return analyticsDataService;
}
