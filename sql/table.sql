-- Begin Tags {{{

-- Create our schema if we haven't already done so.
CREATE SCHEMA IF NOT EXISTS tags AUTHORIZATION frame;

-- Ensure everything we do is within the new tags schema.
SET SCHEMA 'tags';

-- The main tags table.
CREATE TABLE IF NOT EXISTS tags (
	tid bigserial primary key,
	name varchar(128) NOT NULL,
	parent bigint DEFAULT NULL,
	description text,

	UNIQUE ( name )
);

ALTER TABLE IF EXISTS tags OWNER TO frame;

COMMENT ON COLUMN tags.tid IS 'The Tag ID';
COMMENT ON COLUMN tags.name IS 'The actual tag itself, its string name';
COMMENT ON COLUMN tags.description IS 'For more common tags, a description of the tag itself';
COMMENT ON COLUMN tags.parent IS 'For alias tags, when parent is set that ID should be used instead. Allows changing regular tags to aliases';

-- This is set to DEFINER specifically so that you can just give permission to this function without needing to
-- give permission to the table itself.
CREATE OR REPLACE FUNCTION get_tagid(wanted varchar(128)) RETURNS bigint
	LANGUAGE plpgsql SECURITY DEFINER
	AS $$
		DECLARE
			loops integer = 0;
			vtid bigint;
			vparent bigint;
		BEGIN
			-- We always want the tags to be in lower case, makes things a lot easier.
			wanted = lower(wanted);

			-- We loop here because its very possible for us to be called twice at the same time.
			-- So we loop in case someone else inserts the same tag we are trying to at the same time as us.
			LOOP
				-- First, does this tag already exist?
				SELECT tid, parent INTO vtid, vparent FROM tags.tags WHERE name = wanted;
				IF FOUND THEN
					IF vparent IS NOT NULL THEN
						RETURN vparent;
					END IF;
					RETURN vtid;
				END IF;

				-- Ok, it doesn't already exist, so go ahead and add it to the tags table.
				-- Ensure we are in another BEGIN .. END so this failure doesn't cause the function itself to fail.
				BEGIN
					-- Two things we account for here. The insert works, in which case the next loop finds it.
					-- Or the insert fails because it was inserted after our SELECT above (unique_violation), in
					-- which case the next loop also catches the new tid.
					INSERT INTO tags.tags ( name ) VALUES ( wanted );
					EXCEPTION WHEN unique_violation THEN
						-- It was inserted already, so we ignore this and loop.
						NULL;
				END;

				-- We loop too many times already?
				IF loops > 2 THEN
					RAISE EXCEPTION 'Unable to get a tid %', wanted ;
				END IF;
				
				-- Increase our loop count.
				loops := loops + 1;
			END LOOP;
		END
	$$;

ALTER FUNCTION get_tagid(wanted varchar(128)) OWNER TO frame;

COMMENT ON FUNCTION get_tagid(wanted varchar(128)) IS 'This returns the tid of the requested tag, handling aliases and inserting the tag if it doesn''t already exist';

CREATE OR REPLACE FUNCTION get_tagnames(intags bigint[]) RETURNS text[]
	LANGUAGE plpgsql SECURITY DEFINER
	AS $$
		DECLARE
			names text[];
		BEGIN
			SELECT array(
				SELECT
					name
				FROM
					tags.tags
				WHERE
					tid = any(intags)
				ORDER BY
					name
			) INTO names;
			RETURN names;
		END
	$$;

ALTER FUNCTION get_tagnames(intags bigint[]) OWNER TO frame;

-- End Tags }}}

-- Begin Files {{{

-- Create our schema if we haven't already done so.
CREATE SCHEMA IF NOT EXISTS files AUTHORIZATION frame;

-- Ensure everything we do is within the new tags schema.
SET SCHEMA 'files';

CREATE TABLE IF NOT EXISTS hashes (
	hid bigserial PRIMARY KEY,
	hash varchar(128) NOT NULL,

	UNIQUE(  hash )
);

ALTER TABLE IF EXISTS hashes OWNER TO frame;

-- This is set to DEFINER specifically so that you can just give permission to this function without needing to
-- give permission to the table itself.
CREATE OR REPLACE FUNCTION get_hashid(wanted varchar(128)) RETURNS bigint
	LANGUAGE plpgsql SECURITY DEFINER
	AS $$
		DECLARE
			loops integer = 0;
			vhid bigint;
			vparent bigint;
		BEGIN
			-- We always want the tags to be in lower case, makes things a lot easier.
			wanted = lower(wanted);

			-- We loop here because its very possible for us to be called twice at the same time.
			-- So we loop in case someone else inserts the same tag we are trying to at the same time as us.
			LOOP
				-- First, does this tag already exist?
				SELECT hid INTO vhid FROM files.hashes WHERE hash = wanted;
				IF FOUND THEN
					RETURN vhid;
				END IF;

				-- Ok, it doesn't already exist, so go ahead and add it to the tags table.
				-- Ensure we are in another BEGIN .. END so this failure doesn't cause the function itself to fail.
				BEGIN
					-- Two things we account for here. The insert works, in which case the next loop finds it.
					-- Or the insert fails because it was inserted after our SELECT above (unique_violation), in
					-- which case the next loop also catches the new tid.
					INSERT INTO files.hashes ( hash ) VALUES ( wanted );
					EXCEPTION WHEN unique_violation THEN
						-- It was inserted already, so we ignore this and loop.
						NULL;
				END;

				-- We loop too many times already?
				IF loops > 2 THEN
					RAISE EXCEPTION 'Unable to get a hid %', wanted ;
				END IF;

				-- Increase our loop count.
				loops := loops + 1;
			END LOOP;
		END
	$$;

ALTER FUNCTION get_hashid(wanted varchar(128)) OWNER TO frame;

-- A "base" path is a path that one expects to change between servers.
--
-- For example on serveer A you might have "/mnt/external_path", and on server B is might be "/usr/mnt/that_server", yet server C it could be "H:\"
--
-- We do however expect all the paths within this "base" path to be the same between servers, only the mount point being different.
CREATE TABLE IF NOT EXISTS base (
	bid bigserial primary key,
	description text NOT NULL
);

ALTER TABLE IF EXISTS base OWNER TO frame;

COMMENT ON COLUMN base.bid IS 'The Base ID';
COMMENT ON COLUMN base.description IS 'Human-readable description of the base path. Can include things like server name, mount options, service like twitter, etc.';

CREATE TABLE IF NOT EXISTS paths (
	pid bigserial PRIMARY KEY,
	bid bigint NOT NULL, -- The base id

	-- 4096 was chosen as its the largest path allowed by btrfs and ext4, though under NTFS this can be 32k.
	name varchar(4096) NOT NULL,

	-- When the path was last changed
	pathts timestamptz NOT NULL DEFAULT NOW(),

	-- When the sidecar/tagfile was last changed.
	sidets timestamptz NOT NULL DEFAULT NOW(),

	-- Any tags assigned to this specific path.
	-- Can be NULL.
	tags bigint[] DEFAULT NULL,

	-- If this path is enabled or not.
	--
	-- If false then all files using this path are also ignored and treated as if they are disabled.
	enabled boolean NOT NULL DEFAULT true,

	updated timestamptz NOT NULL DEFAULT NOW(),

	FOREIGN KEY ( bid ) REFERENCES base,

	UNIQUE ( bid, name )
);

ALTER TABLE IF EXISTS paths OWNER TO frame;

CREATE OR REPLACE FUNCTION paths_upd() RETURNS trigger
	LANGUAGE plpgsql SECURITY DEFINER
	AS $$
		BEGIN
			IF NEW.pathts != OLD.pathts OR NEW.tags != OLD.tags OR NEW.enabled != OLD.enabled OR NEW.sidets != OLD.sidets THEN
				NEW.updated = NOW();
			END IF;
			RETURN NEW;
		END;
	$$;

ALTER FUNCTION paths_upd() OWNER TO frame ;

CREATE TRIGGER paths_upd BEFORE INSERT OR UPDATE ON files.paths FOR EACH ROW EXECUTE FUNCTION paths_upd();

CREATE TABLE IF NOT EXISTS files (
	fid bigserial PRIMARY KEY,
	pid bigint NOT NULL,

	-- If this file is enabled or not.
	enabled boolean NOT NULL DEFAULT true,

	-- The name of the file itself within the containing path (pid).
	-- btrfs, ext4 and NTFS set the limit on the name of a file to 255 bytes.
	--
	-- This name is *not* unique, as many files can have the exact same name in multiple paths.
	-- The name is also expected to be almost random compared to the contents of the file.
	name varchar(255) NOT NULL,

	-- When the file was last changed.
	filets timestamptz NOT NULL DEFAULT NOW(),

	-- When the sidecar/tagfile was last changed.
	sidets timestamptz NOT NULL DEFAULT NOW(),

	-- Sidecar tags, loaded from a .txt sidecar file.
	--
	-- Can be empty so long as either the file or the path has at least 1 tag.
	sidetags bigint[],

	-- The hashed ID of the file, hashed using SHA-512 (default).
	--
	-- This is how the file itself is looked up once processed.
	--
	-- Note that the hash is *not* unique within the database, but the file *is* unique.
	--
	-- Meaning two files resulting in the same hash will point to the same file on disk.
	--
	-- This simply means that the input has duplicate files with different names, but there was only 1 output.
	--
	-- This is expected, as if for example you follow the same person say on Twitter, Instagram, etc.
	-- They are going to post the same photos on both.
	--
	-- To account for this the final "merge" table merges handles this, and merges the tags of those files with the same hashes.
	hid bigint NOT NULL,

	-- The calculated tags for the file.
	-- This is the combined path, file and sidecar tags into 1 column.
	--
	-- A file *must* have at least 1 tag to be added, otherwise there is no way to possibly choose the file.
	tags bigint[] NOT NULL,

	updated timestamptz NOT NULL DEFAULT NOW(),

	UNIQUE( pid, name ),

	FOREIGN KEY ( hid ) REFERENCES hashes,

	FOREIGN KEY ( pid ) REFERENCES paths
);

ALTER TABLE IF EXISTS files OWNER TO frame;

CREATE OR REPLACE FUNCTION files_upd() RETURNS trigger
	LANGUAGE plpgsql SECURITY DEFINER
	AS $$
		BEGIN
			IF NEW.filets != OLD.filets OR NEW.sidets != OLD.sidets OR NEW.sidetags != OLD.sidetags OR NEW.tags != OLD.tags OR NEW.hid != OLD.hid THEN
				NEW.updated = NOW();
			END IF;
			RETURN NEW;
		END;
	$$;

ALTER FUNCTION files_upd() OWNER TO frame ;

CREATE TRIGGER files_upd BEFORE INSERT OR UPDATE ON files.files FOR EACH ROW EXECUTE FUNCTION files_upd();

CREATE TABLE IF NOT EXISTS merged (
	hid bigint NOT NULL,

	-- These are the combined tags of any files with the same hash *as well as* the tags assigned to the directories (if any)
	-- those files are from.
	tags bigint[] NOT NULL,

	updated timestamptz NOT NULL DEFAULT NOW(),

	blocked bool NOT NULL DEFAULT false,
	enabled bool NOT NULL DEFAULT true,

	FOREIGN KEY ( hid ) REFERENCES hashes,
	UNIQUE(  hid )
);

ALTER TABLE IF EXISTS merged OWNER TO frame;

CREATE OR REPLACE FUNCTION merged_upd() RETURNS trigger
	LANGUAGE plpgsql SECURITY DEFINER
	AS $$
		BEGIN
			IF NEW.blocked != OLD.blocked OR NEW.tags != OLD.tags OR NEW.enabled != OLD.enabled THEN
				NEW.updated = NOW();
			END IF;
			RETURN NEW;
		END;
	$$;

ALTER FUNCTION merged_upd() OWNER TO frame ;

CREATE TRIGGER merged_upd BEFORE INSERT OR UPDATE ON files.merged FOR EACH ROW EXECUTE FUNCTION merged_upd();

CREATE OR REPLACE VIEW files.merged_names AS SELECT hid, get_tagnames(tags) AS tags, updated, blocked, enabled FROM files.merged ;

ALTER VIEW IF EXISTS merged_names OWNER TO frame;

-- End Files }}}

