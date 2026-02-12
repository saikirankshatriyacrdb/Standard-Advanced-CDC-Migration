CREATE TABLE public.login (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	username STRING NULL,
	username_search_hash STRING NULL,
	password_secret_hash STRING NULL,
	password_set_on TIMESTAMPTZ NULL,
	status public."UserStatus" NULL,
	type public."UserType" NULL,
	last_login_date DATE NULL,
	failed_auth_cnt INT8 NULL,
	account_non_locked BOOL NULL,
	failed_password_reset_cnt INT8 NULL,
	last_unlock_attempt_date DATE NULL,
	unlock_attempts INT8 NULL,
	last_validate_username_attempt_date DATE NULL,
	validate_username_attempts INT8 NULL,
	deactivation_time TIMESTAMPTZ NULL,
	initial_user_id INT8 NULL,
	uuid UUID NULL,
	tfa_enabled BOOL NULL,
	phone_authentication_enabled BOOL NOT NULL DEFAULT false,
	is_temporary BOOL NULL,
	roles_synced_to_access_control BOOL NULL,
	access_control_enabled BOOL NULL,
	external_idp_user_id STRING NULL,
	external_idp_id STRING NULL,
	superseded_by_login_id INT8 NULL,
	CONSTRAINT login_pkey PRIMARY KEY (id ASC),
	UNIQUE INDEX login_uuid_key (uuid ASC),
	INDEX login_username_search_hash_idx (username_search_hash ASC),
	INDEX login_username_and_type_not_deactivated_idx (username_search_hash ASC, type ASC),
	INDEX login_status_idx (status ASC),
	INDEX login_is_temporary_idx (is_temporary ASC),
	INDEX login_type_employee_idx (type ASC),
	INDEX login_external_idp_metadata_idx (external_idp_id ASC, external_idp_user_id ASC),
	INDEX login_external_idp_metadata_not_deactivated_idx (external_idp_id ASC, external_idp_user_id ASC),
	CONSTRAINT temporary_logins_cannot_have_passwords CHECK (NOT ((is_temporary = true) AND (password_secret_hash IS NOT NULL)))
);
CREATE TABLE public.login_role (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	"role" public."Role" NULL,
	create_date TIMESTAMPTZ NULL,
	delete_date TIMESTAMPTZ NULL,
	CONSTRAINT login_role_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.api_client (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	client_id STRING NOT NULL,
	client_secret_secret_hash STRING NOT NULL,
	enabled BOOL NOT NULL DEFAULT true,
	roles JSONB NOT NULL,
	attributes JSONB NOT NULL,
	access_token_expiry_time_seconds INT8 NOT NULL,
	refresh_token_expiry_time_seconds INT8 NOT NULL,
	trusted BOOL NULL DEFAULT false,
	grant_types JSONB NULL,
	scopes JSONB NULL,
	is_external BOOL NULL DEFAULT false,
	CONSTRAINT api_client_pkey PRIMARY KEY (id ASC),
	UNIQUE INDEX api_client_client_id_key (client_id ASC)
);
CREATE TABLE public.access_token (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	jti UUID NOT NULL,
	issued_at TIMESTAMPTZ NULL,
	api_client_id INT8 NOT NULL,
	login_id INT8 NOT NULL,
	scope JSONB NULL,
	expiration_date TIMESTAMPTZ NOT NULL,
	revocation_date TIMESTAMPTZ NULL,
	client_ip STRING NULL,
	target_scope JSONB NULL,
	authentication_level public."AuthenticationLevel" NULL,
	all_factor_authenticated BOOL NULL,
	refreshed_at TIMESTAMPTZ NULL,
	CONSTRAINT access_token_pkey PRIMARY KEY (id ASC),
	UNIQUE INDEX access_token_jti_key (jti ASC),
	INDEX access_token_login_id_idx (login_id ASC)
);
CREATE TABLE public.reset_log (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	reset_token STRING NULL,
	expiration_date DATE NULL,
	status public."TokenStatus" NULL,
	token_purpose public."TokenPurpose" NULL,
	uuid UUID NULL,
	CONSTRAINT reset_log_pkey PRIMARY KEY (id ASC),
	INDEX reset_log_uuid_idx (uuid ASC),
	INDEX reset_log_login_purpose_status_idx (login_id ASC, token_purpose ASC, status ASC)
);
CREATE TABLE public.login_username_log (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	username STRING NULL,
	username_hash STRING NULL,
	username_search_hash STRING NULL,
	email_domain STRING NULL,
	create_date TIMESTAMPTZ NULL,
	CONSTRAINT login_username_log_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.username_change_request (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	new_username STRING NULL,
	username_search_hash STRING NULL,
	status public."UsernameChangeRequestStatus" NULL,
	reason STRING NULL,
	notes STRING NULL,
	CONSTRAINT username_change_request_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.webauthn_credential (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	credential_id STRING NOT NULL,
	credential_type STRING NOT NULL,
	transports JSONB NOT NULL,
	public_key_cose STRING NOT NULL,
	signature_count INT8 NOT NULL,
	deactivation_time TIMESTAMPTZ NULL,
	assurance_level INT8 NOT NULL,
	CONSTRAINT webauthn_credential_pkey PRIMARY KEY (id ASC),
	UNIQUE INDEX webauthn_credential_credential_id_key (credential_id ASC)
);
CREATE TABLE public.previous_password (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	password_secret_hash STRING NOT NULL,
	set_on TIMESTAMPTZ NOT NULL,
	CONSTRAINT previous_password_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.role_privilege (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	"role" public."Role" NULL,
	privileges JSONB NULL,
	CONSTRAINT role_privilege_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.login_dependency (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	type STRING NULL,
	key STRING NULL,
	risk_level INT8 NULL,
	deactivation_at TIMESTAMPTZ NULL,
	CONSTRAINT login_dependency_pkey PRIMARY KEY (id ASC),
	INDEX login_dependency_login_id_idx (login_id ASC),
	INDEX login_dependency_login_type_key_idx (login_id ASC, type ASC, key ASC)
);
CREATE TABLE public.supersede_login_authorization (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	initiator_login_id INT8 NOT NULL,
	login_id INT8 NOT NULL,
	expiration_time TIMESTAMPTZ NULL,
	CONSTRAINT supersede_login_authorization_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.login_create (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	source STRING NULL,
	create_date TIMESTAMPTZ NULL,
	CONSTRAINT login_create_pkey PRIMARY KEY (id ASC),
	UNIQUE INDEX login_create_login_id_key (login_id ASC)
);
CREATE TABLE public.login_username_freeze (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	context_type STRING NULL,
	context_key STRING NULL,
	deactivation_time TIMESTAMPTZ NULL,
	CONSTRAINT login_username_freeze_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.okta_user_sync_job (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	last_updated_le TIMESTAMPTZ NULL,
	status STRING NULL,
	CONSTRAINT okta_user_sync_job_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.registrable_authentication_factor (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	authentication_factor_type public."AuthenticationFactorType" NULL,
	CONSTRAINT registrable_authentication_factor_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.login_force_set_credentials (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	force_set_password BOOL NULL,
	create_date TIMESTAMPTZ NULL,
	CONSTRAINT login_force_set_credentials_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.login_invitation (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	username_search_hash STRING NULL,
	realm public."UserType" NULL,
	status public."LoginInvitationStatus" NULL,
	create_date TIMESTAMPTZ NULL,
	CONSTRAINT login_invitation_pkey PRIMARY KEY (id ASC)
);
CREATE TABLE public.device (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	login_id INT8 NOT NULL,
	name STRING NULL,
	system_name STRING NULL,
	type STRING NULL,
	platform_type public."PlatformType" NULL,
	unique_id_search_hash STRING NULL,
	CONSTRAINT device_pkey PRIMARY KEY (id ASC),
	INDEX device_login_id_idx (login_id ASC)
);
CREATE TABLE public.device_token (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	uuid UUID NOT NULL,
	login_id INT8 NOT NULL,
	device_id INT8 NULL,
	last_access_date TIMESTAMPTZ NULL,
	expiration_date TIMESTAMPTZ NOT NULL,
	revocation_date TIMESTAMPTZ NULL,
	CONSTRAINT device_token_pkey PRIMARY KEY (id ASC),
	UNIQUE INDEX device_token_uuid_key (uuid ASC),
	INDEX devicetoken_login_id_idx (login_id ASC)
);
CREATE TABLE public.device_token_authentication_factor (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	device_token_id INT8 NOT NULL,
	authentication_factor_type public."AuthenticationFactorType" NOT NULL,
	revocation_date TIMESTAMPTZ NULL,
	CONSTRAINT device_token_authentication_factor_pkey PRIMARY KEY (id ASC),
	UNIQUE INDEX uq_devicetoken_factortype (device_token_id ASC, authentication_factor_type ASC)
);
CREATE TABLE public.items (
	id UUID NOT NULL DEFAULT gen_random_uuid(),
	embedding VECTOR(1536) NULL,
	created_at TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
	CONSTRAINT items_pkey PRIMARY KEY (id ASC),
	VECTOR INDEX items_vec_idx (embedding vector_l2_ops)
);
CREATE TABLE public.item_vectors (
	item_id UUID NOT NULL,
	embedding VECTOR(1536) NULL,
	CONSTRAINT item_vectors_pkey PRIMARY KEY (item_id ASC)
);
ALTER TABLE public.login ADD CONSTRAINT fk_login_superseded_by FOREIGN KEY (superseded_by_login_id) REFERENCES public.login(id);
ALTER TABLE public.login_role ADD CONSTRAINT fk_loginrole_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.access_token ADD CONSTRAINT fk_accesstoken_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.reset_log ADD CONSTRAINT fk_resetlog_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.login_username_log ADD CONSTRAINT fk_loginusernamelog_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.username_change_request ADD CONSTRAINT fk_usernamechangerequest_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.webauthn_credential ADD CONSTRAINT fk_webauthn_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.previous_password ADD CONSTRAINT fk_previouspassword_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.login_dependency ADD CONSTRAINT fk_logindependency_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.supersede_login_authorization ADD CONSTRAINT fk_supersede_initiator_login FOREIGN KEY (initiator_login_id) REFERENCES public.login(id);
ALTER TABLE public.supersede_login_authorization ADD CONSTRAINT fk_supersede_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.login_create ADD CONSTRAINT fk_logincreate_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.login_username_freeze ADD CONSTRAINT fk_loginusernamefreeze_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.registrable_authentication_factor ADD CONSTRAINT fk_regauthfactor_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.login_force_set_credentials ADD CONSTRAINT fk_loginforceset_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.device ADD CONSTRAINT fk_device_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.device_token ADD CONSTRAINT fk_devicetoken_login FOREIGN KEY (login_id) REFERENCES public.login(id);
ALTER TABLE public.device_token ADD CONSTRAINT fk_devicetoken_device FOREIGN KEY (device_id) REFERENCES public.device(id);
ALTER TABLE public.device_token_authentication_factor ADD CONSTRAINT fk_deviceauthfactor_devicetoken FOREIGN KEY (device_token_id) REFERENCES public.device_token(id);
ALTER TABLE public.item_vectors ADD CONSTRAINT item_vectors_item_id_fkey FOREIGN KEY (item_id) REFERENCES public.items(id) ON DELETE CASCADE;
-- Validate foreign key constraints. These can fail if there was unvalidated data during the SHOW CREATE ALL TABLES
ALTER TABLE public.login VALIDATE CONSTRAINT fk_login_superseded_by;
ALTER TABLE public.login_role VALIDATE CONSTRAINT fk_loginrole_login;
ALTER TABLE public.access_token VALIDATE CONSTRAINT fk_accesstoken_login;
ALTER TABLE public.reset_log VALIDATE CONSTRAINT fk_resetlog_login;
ALTER TABLE public.login_username_log VALIDATE CONSTRAINT fk_loginusernamelog_login;
ALTER TABLE public.username_change_request VALIDATE CONSTRAINT fk_usernamechangerequest_login;
ALTER TABLE public.webauthn_credential VALIDATE CONSTRAINT fk_webauthn_login;
ALTER TABLE public.previous_password VALIDATE CONSTRAINT fk_previouspassword_login;
ALTER TABLE public.login_dependency VALIDATE CONSTRAINT fk_logindependency_login;
ALTER TABLE public.supersede_login_authorization VALIDATE CONSTRAINT fk_supersede_initiator_login;
ALTER TABLE public.supersede_login_authorization VALIDATE CONSTRAINT fk_supersede_login;
ALTER TABLE public.login_create VALIDATE CONSTRAINT fk_logincreate_login;
ALTER TABLE public.login_username_freeze VALIDATE CONSTRAINT fk_loginusernamefreeze_login;
ALTER TABLE public.registrable_authentication_factor VALIDATE CONSTRAINT fk_regauthfactor_login;
ALTER TABLE public.login_force_set_credentials VALIDATE CONSTRAINT fk_loginforceset_login;
ALTER TABLE public.device VALIDATE CONSTRAINT fk_device_login;
ALTER TABLE public.device_token VALIDATE CONSTRAINT fk_devicetoken_login;
ALTER TABLE public.device_token VALIDATE CONSTRAINT fk_devicetoken_device;
ALTER TABLE public.device_token_authentication_factor VALIDATE CONSTRAINT fk_deviceauthfactor_devicetoken;
ALTER TABLE public.item_vectors VALIDATE CONSTRAINT item_vectors_item_id_fkey;
