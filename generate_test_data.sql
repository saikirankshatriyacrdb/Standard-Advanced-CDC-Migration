-- ============================================================
-- Generate test data for all tables
-- Respects FK dependency order
-- ============================================================

-- PHASE 1: Root / independent tables
-- ============================================================

-- 1. login (500 rows) — root table, most others reference it
INSERT INTO login (id, username, username_search_hash, status, type, last_login_date, failed_auth_cnt, account_non_locked, phone_authentication_enabled, uuid, tfa_enabled, is_temporary)
SELECT
  i,
  'user_' || i::STRING || '@example.com',
  md5('user_' || i::STRING),
  (ARRAY['ACTIVE', 'DEACTIVATED', 'LOCKED'])[1 + (i % 3)]::public."UserStatus",
  (ARRAY['CONSUMER', 'EMPLOYEE', 'SYSTEM', 'PARTNER'])[1 + (i % 4)]::public."UserType",
  current_date() - (i % 365)::INT,
  i % 5,
  (i % 10 > 0),
  false,
  gen_random_uuid(),
  (i % 3 = 0),
  false
FROM generate_series(1, 500) AS g(i);

-- 2. api_client (50 rows)
INSERT INTO api_client (id, client_id, client_secret_secret_hash, enabled, roles, attributes, access_token_expiry_time_seconds, refresh_token_expiry_time_seconds, trusted, is_external)
SELECT
  i,
  'client_' || i::STRING,
  md5('secret_' || i::STRING),
  (i % 5 > 0),
  '["USER_ROLE"]'::JSONB,
  '{"app": "test"}'::JSONB,
  3600 + (i * 60),
  86400 + (i * 600),
  (i % 3 = 0),
  (i % 10 = 0)
FROM generate_series(1, 50) AS g(i);

-- 3. role_privilege (3 rows)
INSERT INTO role_privilege (id, "role", privileges) VALUES
  (1, 'ADMIN_ROLE', '["read", "write", "admin"]'),
  (2, 'USER_ROLE', '["read", "write"]'),
  (3, 'ANOTHER_ROLE', '["read"]');

-- 4. okta_user_sync_job (5 rows)
INSERT INTO okta_user_sync_job (id, last_updated_le, status)
SELECT i, now() - (i || ' hours')::INTERVAL, (ARRAY['RUNNING', 'COMPLETED', 'FAILED'])[1 + (i % 3)]
FROM generate_series(1, 5) AS g(i);

-- 5. login_invitation (100 rows)
INSERT INTO login_invitation (id, username_search_hash, realm, status, create_date)
SELECT
  i,
  md5('invite_' || i::STRING),
  (ARRAY['CONSUMER', 'EMPLOYEE', 'SYSTEM', 'PARTNER'])[1 + (i % 4)]::public."UserType",
  (ARRAY['PENDING', 'COMPLETED', 'CANCELLED'])[1 + (i % 3)]::public."LoginInvitationStatus",
  now() - (i || ' hours')::INTERVAL
FROM generate_series(1, 100) AS g(i);

-- PHASE 2: Tables that reference login
-- ============================================================

-- 6. login_role (1000 rows — multiple roles per login)
INSERT INTO login_role (id, login_id, "role", create_date)
SELECT
  i,
  1 + (i % 500),
  (ARRAY['ADMIN_ROLE', 'USER_ROLE', 'ANOTHER_ROLE'])[1 + (i % 3)]::public."Role",
  now() - (i || ' minutes')::INTERVAL
FROM generate_series(1, 1000) AS g(i);

-- 7. access_token (800 rows)
INSERT INTO access_token (id, jti, issued_at, api_client_id, login_id, scope, expiration_date, authentication_level, all_factor_authenticated)
SELECT
  i,
  gen_random_uuid(),
  now() - (i || ' minutes')::INTERVAL,
  1 + (i % 50),
  1 + (i % 500),
  '["openid", "profile"]'::JSONB,
  now() + ((i * 10) || ' minutes')::INTERVAL,
  (ARRAY['NONE', 'LOW', 'SUBSTANTIAL', 'HIGH', 'VERY_HIGH'])[1 + (i % 5)]::public."AuthenticationLevel",
  (i % 2 = 0)
FROM generate_series(1, 800) AS g(i);

-- 8. reset_log (300 rows)
INSERT INTO reset_log (id, login_id, reset_token, expiration_date, status, token_purpose, uuid)
SELECT
  i,
  1 + (i % 500),
  md5('reset_' || i::STRING),
  current_date() + (i % 30),
  (ARRAY['PENDING', 'USED', 'EXPIRED'])[1 + (i % 3)]::public."TokenStatus",
  (ARRAY['PASSWORD_RESET', 'EMAIL_VERIFICATION', 'PHONE_VERIFICATION'])[1 + (i % 3)]::public."TokenPurpose",
  gen_random_uuid()
FROM generate_series(1, 300) AS g(i);

-- 9. login_username_log (500 rows)
INSERT INTO login_username_log (id, login_id, username, username_hash, username_search_hash, email_domain, create_date)
SELECT
  i,
  1 + (i % 500),
  'user_' || (1 + (i % 500))::STRING || '@example.com',
  md5('user_' || i::STRING),
  md5('search_' || i::STRING),
  'example.com',
  now() - (i || ' hours')::INTERVAL
FROM generate_series(1, 500) AS g(i);

-- 10. username_change_request (200 rows)
INSERT INTO username_change_request (id, login_id, new_username, username_search_hash, status, reason, notes)
SELECT
  i,
  1 + (i % 500),
  'newuser_' || i::STRING || '@example.com',
  md5('newuser_' || i::STRING),
  (ARRAY['PENDING', 'APPROVED', 'REJECTED', 'COMPLETED'])[1 + (i % 4)]::public."UsernameChangeRequestStatus",
  'Requested username change',
  'Note ' || i::STRING
FROM generate_series(1, 200) AS g(i);

-- 11. webauthn_credential (150 rows)
INSERT INTO webauthn_credential (id, login_id, credential_id, credential_type, transports, public_key_cose, signature_count, assurance_level)
SELECT
  i,
  1 + (i % 500),
  'cred_' || i::STRING || '_' || gen_random_uuid()::STRING,
  'public-key',
  '["usb", "nfc"]'::JSONB,
  md5('pubkey_' || i::STRING),
  i * 10,
  (i % 3) + 1
FROM generate_series(1, 150) AS g(i);

-- 12. previous_password (600 rows)
INSERT INTO previous_password (id, login_id, password_secret_hash, set_on)
SELECT
  i,
  1 + (i % 500),
  md5('oldpass_' || i::STRING),
  now() - (i || ' days')::INTERVAL
FROM generate_series(1, 600) AS g(i);

-- 13. login_dependency (400 rows)
INSERT INTO login_dependency (id, login_id, type, key, risk_level, deactivation_at)
SELECT
  i,
  1 + (i % 500),
  (ARRAY['SUBSCRIPTION', 'PAYMENT', 'API_KEY', 'LICENSE'])[1 + (i % 4)],
  'dep_key_' || i::STRING,
  (i % 5) + 1,
  CASE WHEN i % 10 = 0 THEN now() ELSE NULL END
FROM generate_series(1, 400) AS g(i);

-- 14. supersede_login_authorization (100 rows)
INSERT INTO supersede_login_authorization (id, initiator_login_id, login_id, expiration_time)
SELECT
  i,
  1 + (i % 250),
  251 + (i % 250),
  now() + (i || ' hours')::INTERVAL
FROM generate_series(1, 100) AS g(i);

-- 15. login_create (500 rows — one per login, unique constraint)
INSERT INTO login_create (id, login_id, source, create_date)
SELECT
  i,
  i,
  (ARRAY['WEB', 'MOBILE', 'API', 'MIGRATION', 'ADMIN'])[1 + (i % 5)],
  now() - (i || ' days')::INTERVAL
FROM generate_series(1, 500) AS g(i);

-- 16. login_username_freeze (200 rows)
INSERT INTO login_username_freeze (id, login_id, context_type, context_key, deactivation_time)
SELECT
  i,
  1 + (i % 500),
  (ARRAY['FRAUD', 'COMPLIANCE', 'SUPPORT'])[1 + (i % 3)],
  'ctx_' || i::STRING,
  CASE WHEN i % 5 = 0 THEN now() ELSE NULL END
FROM generate_series(1, 200) AS g(i);

-- 17. registrable_authentication_factor (300 rows)
INSERT INTO registrable_authentication_factor (id, login_id, authentication_factor_type)
SELECT
  i,
  1 + (i % 500),
  (ARRAY['PASSWORD', 'PHONE_OTP', 'EMAIL_OTP', 'DEVICE_TOKEN', 'WEBAUTHN', 'TOTP', 'DELEGATED'])[1 + (i % 7)]::public."AuthenticationFactorType"
FROM generate_series(1, 300) AS g(i);

-- 18. login_force_set_credentials (250 rows)
INSERT INTO login_force_set_credentials (id, login_id, force_set_password, create_date)
SELECT
  i,
  1 + (i % 500),
  (i % 2 = 0),
  now() - (i || ' hours')::INTERVAL
FROM generate_series(1, 250) AS g(i);

-- PHASE 3: Tables with deeper FK dependencies
-- ============================================================

-- 19. device (400 rows) — references login
INSERT INTO device (id, login_id, name, system_name, type, platform_type, unique_id_search_hash)
SELECT
  i,
  1 + (i % 500),
  'Device ' || i::STRING,
  (ARRAY['iPhone 15', 'Pixel 8', 'MacBook Pro', 'Galaxy S24'])[1 + (i % 4)],
  (ARRAY['mobile', 'desktop', 'tablet'])[1 + (i % 3)],
  (ARRAY['IOS', 'ANDROID', 'WEB'])[1 + (i % 3)]::public."PlatformType",
  md5('device_' || i::STRING)
FROM generate_series(1, 400) AS g(i);

-- 20. device_token (600 rows) — references login AND device
INSERT INTO device_token (id, uuid, login_id, device_id, last_access_date, expiration_date)
SELECT
  i,
  gen_random_uuid(),
  1 + (i % 500),
  1 + (i % 400),
  now() - (i || ' hours')::INTERVAL,
  now() + ((i * 24) || ' hours')::INTERVAL
FROM generate_series(1, 600) AS g(i);

-- 21. device_token_authentication_factor (500 rows) — references device_token
-- Use unique (device_token_id, authentication_factor_type) pairs
INSERT INTO device_token_authentication_factor (id, device_token_id, authentication_factor_type)
SELECT
  i,
  1 + ((i - 1) / 7),
  (ARRAY['PASSWORD', 'PHONE_OTP', 'EMAIL_OTP', 'DEVICE_TOKEN', 'WEBAUTHN', 'TOTP', 'DELEGATED'])[1 + ((i - 1) % 7)]::public."AuthenticationFactorType"
FROM generate_series(1, 500) AS g(i)
WHERE (1 + ((i - 1) / 7)) <= 600;

-- 22. item_vectors (200 rows) — references items
-- Use existing item IDs
INSERT INTO item_vectors (item_id, embedding)
SELECT id, embedding
FROM items
ORDER BY id
LIMIT 200;
