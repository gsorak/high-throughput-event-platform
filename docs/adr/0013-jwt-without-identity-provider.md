# ADR-0013: Tenant identity from self-issued JWTs, without an identity provider

- Status: Accepted (2026-10-27); implemented in Stage 2. The plan is configuration, not a claim (see Decision).
- Stage: 2
- Assessment: **Provisional.** The plan names "static RSA key + mint script". Keycloak was excluded in one line.
  API keys and mTLS were not considered.

## Problem

- Replace the Stage 1 `X-Tenant-Id` header, which is unauthenticated ([ADR-0012](0012-pooled-multi-tenancy.md)).
  Every request to `ingestion-api` and to `GET /v1/stats` on `event-processor` must carry a verifiable tenant.
- The callers are machine clients (tenant back ends), not interactive users.
- Validation must not add a network call per request on the hot path.

## Decision

- Both services act as Spring Security OAuth2 resource servers. They validate RS256 JWTs offline against a configured
  RSA public key (`security.jwt.*`): signature, `exp` (required), `iss`, `aud`, and a `tenant_id` claim that must be
  a valid tenant id. The tenant comes only from `tenant_id`; an `X-Tenant-Id` header is ignored. Requests without a
  valid token get 401.
- The plan is not a claim. `ingestion-api` maps tenants to plans in configuration (`rate-limit.tenants`), so a plan
  change needs no new token.
- `java scripts/DevJwt.java keys` writes a key pair to `.local/jwt/` (ignored by Git); `token <tenant> [minutes]`
  mints a token, valid 60 minutes by default. **The private key is never committed.** Tests generate a key pair per
  run.
- There is no token endpoint, revocation or key rotation in Stage 2.

## Alternatives considered

1. **Keycloak** (or another OpenID Connect provider) in Compose. It provides client-credentials flow, JWKS rotation and
   an admin UI.
2. **Spring Authorization Server.** A small token service in this repository that issues client-credentials tokens
   and publishes a JWKS endpoint.
3. **API keys.** An opaque per-tenant secret in a header, looked up in a store (hashed) or cached in memory.
4. **mTLS.** The client certificate identifies the tenant, and TLS is terminated at nginx or the ALB.

## Comparison

| Criterion                    | **Static key + mint script** | Keycloak                             | Spring Authorization Server          | API keys                           | mTLS                                    |
| ---------------------------- | ---------------------------- | ------------------------------------ | ------------------------------------ | ---------------------------------- | --------------------------------------- |
| Per-request cost             | Signature check, no I/O      | Same (JWKS cached)                   | Same (JWKS cached)                   | Store or cache lookup              | Handshake per connection                |
| Revocation                   | **None** until expiry        | Short expiry; introspection possible | Short expiry; introspection possible | Immediate (delete the key)         | Certificate revocation lists or OCSP    |
| Key rotation                 | Manual redeploy              | Built in (JWKS)                      | Built in (JWKS)                      | Per key                            | Per certificate                         |
| New infrastructure           | None                         | Keycloak plus its database           | One more Spring service              | A key store (or config)            | A private CA                            |
| Fit for machine clients      | Good                         | Good (client credentials)            | Good (client credentials)            | **Most common for ingestion APIs** | Good, but harder to operate for tenants |
| Shows the production pattern | Partly: validation only      | Yes                                  | Yes                                  | Yes                                | Yes                                     |

## Rationale

- The resource-server side (validate signature, expiry, issuer and audience, then map claims to a tenant) is the same
  code whatever issues the tokens. Swapping the static key for a JWKS URI later is a configuration change.
- No additional service to run, start in tests or keep available.
- Plans stay server-side: rate limits ([ADR-0014](0014-shared-rate-limit-state.md)) read them from configuration,
  which needs no lookup either.

## Consequences

- **Tokens cannot be revoked.** A leaked token is valid until it expires, so token lifetime must be short (minutes to
  hours). The mint script must not issue long-lived tokens by default.
- Key rotation is not exercised. The first rotation in a real deployment would be untested.
- Committing the private key would let anyone mint tokens for any tenant. A secret-scanning check is recommended
  before publication.
- Both services need the same issuer, audience and key configuration.

## Reconsider when

- Before any deployment that real tenants use (Stage 6), move to a real issuer (Cognito, Keycloak or Spring
  Authorization Server) with JWKS rotation.
- If tenants integrate server to server and do not have an OAuth client, **API keys may be the more realistic
  choice.**
- If immediate revocation is required, use API keys or token introspection.

## References

- RFC 7519, JSON Web Token: https://www.rfc-editor.org/rfc/rfc7519
- RFC 6750, Bearer Token Usage: https://www.rfc-editor.org/rfc/rfc6750
- Spring Security, OAuth2 resource server with JWT:
  https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/jwt.html
- Spring Authorization Server:
  https://docs.spring.io/spring-security/reference/servlet/oauth2/authorization-server/index.html
- Keycloak documentation: https://www.keycloak.org/documentation
