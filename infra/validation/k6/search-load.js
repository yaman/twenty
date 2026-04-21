// Tests search endpoint under concurrent load

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
import { BASE_URL, headers, graphql } from './lib/config.js';

const errorRate = new Rate('search_errors');

export const options = {
  stages: [
    { duration: '30s', target: 5 },
    { duration: '2m', target: 30 },
    { duration: '2m', target: 30 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<3000'],
    search_errors: ['rate<0.05'],
  },
};

const SEARCH_TERMS = [
  'acme', 'global', 'tech', 'consulting', 'john',
  'smith', 'engineering', 'marketing', 'sales', 'support',
];

export default function () {
  const term = SEARCH_TERMS[Math.floor(Math.random() * SEARCH_TERMS.length)];

  const body = graphql(`
    query SearchCompanies($filter: CompanyFilterInput) {
      companies(filter: $filter, first: 10) {
        edges {
          node { id name }
        }
      }
    }
  `, {
    filter: {
      name: { like: `%${term}%` }
    }
  });

  const res = http.post(`${BASE_URL}/api`, body, { headers });

  const success = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  errorRate.add(!success);
  sleep(0.2);
}
