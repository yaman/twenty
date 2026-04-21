// Shared configuration for k6 load tests
// Override via env vars: K6_BASE_URL, K6_AUTH_TOKEN

export const BASE_URL = __ENV.K6_BASE_URL || 'http://localhost:3000';
export const AUTH_TOKEN = __ENV.K6_AUTH_TOKEN || '';

export const headers = {
  'Content-Type': 'application/json',
  ...(AUTH_TOKEN ? { 'Authorization': `Bearer ${AUTH_TOKEN}` } : {}),
};

export function graphql(query, variables = {}) {
  return JSON.stringify({ query, variables });
}
