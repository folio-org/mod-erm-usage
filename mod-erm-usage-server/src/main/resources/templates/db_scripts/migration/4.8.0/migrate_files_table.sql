-- If table files with a column jsonb exists, then execute the migration:
-- 1. A new table files_tmp with a column data of type BYTEA is created.
-- 2. The data from the jsonb column is decoded from base64 and inserted into the data column.
-- 3. The files table is dropped.
-- 4. The files_tmp table is renamed to files.
DO $$
BEGIN
  IF EXISTS (
      SELECT 1 FROM information_schema.columns WHERE
          table_schema = '${myuniversity}_${mymodule}' AND
          table_name = 'files' AND
          column_name = 'jsonb'
  ) THEN
      CREATE TABLE IF NOT EXISTS files_tmp(id UUID PRIMARY KEY, data BYTEA NOT NULL);
      INSERT INTO files_tmp (id, data)
        SELECT id, decode(jsonb->>'data', 'base64') FROM files;
      DROP TABLE files CASCADE;
      ALTER TABLE files_tmp RENAME TO files;
      ALTER TABLE files_tmp_pkey RENAME TO files_pkey;
  END IF;
END $$;
