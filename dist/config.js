let config = null;
export function configure(c) {
    config = c;
}
export function getConfig() {
    if (!config) {
        throw new Error('tinyland-analytics-data: call configure() before use');
    }
    return config;
}
export function resetConfig() {
    config = null;
}
