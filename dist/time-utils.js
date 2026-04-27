export function parseTimeRange(timeRange, logger) {
    const match = timeRange.match(/^(\d+)([smhd])$/);
    if (!match) {
        if (logger) {
            logger.warn({ timeRange }, 'Invalid time range format, defaulting to 24h');
        }
        return 24 * 60 * 60 * 1000;
    }
    const value = parseInt(match[1]);
    const unit = match[2];
    const multipliers = {
        s: 1000,
        m: 60 * 1000,
        h: 60 * 60 * 1000,
        d: 24 * 60 * 60 * 1000,
    };
    return value * multipliers[unit];
}
