/**
 * Vitest Configuration for @tinyland-inc/tinyland-analytics-data
 *
 * Works in three modes:
 *   1. Standalone:  cd packages/tinyland-analytics-data && pnpm test
 *   2. Workspace:   vitest run --project=tinyland-analytics-data (from root)
 *   3. Bazel:       bazel test //packages/tinyland-analytics-data:test
 */

import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  root: __dirname,
  test: {
    name: 'tinyland-analytics-data',
    root: __dirname,
    globals: true,
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    pool: 'forks',
    isolate: true,
    testTimeout: 10000,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      reportsDirectory: './coverage',
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.d.ts'],
      thresholds: {
        statements: 60,
        branches: 55,
        functions: 60,
        lines: 60,
      },
    },
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, './src'),
    },
  },
});
