ALTER TABLE file             MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE project          MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE sample           MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE lane             MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE library_request  MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE seq_request      MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE library          MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE mapstats         MODIFY changed DATETIME DEFAULT CURRENT_TIMESTAMP;
update schema_version set schema_version=31;