--liquibase formatted sql
--changeset judo:postgresql-create-function-min-uuid dbms:postgresql  logicalFilePath:postgresql-create-function-min-uuid stripComments:true splitStatements:false failOnError:true
   CREATE FUNCTION min_uuid(uuid, uuid)
    RETURNS uuid AS $$
    BEGIN
        -- if they're both null, return null
        IF $2 IS NULL AND $1 IS NULL THEN
            RETURN NULL ;
        END IF;

        -- if just 1 is null, return the other
        IF $2 IS NULL THEN
            RETURN $1;
        END IF ;
        IF $1 IS NULL THEN
            RETURN $2;
          END IF;

        -- neither are null, return the smaller one
        IF $1 > $2 THEN
            RETURN $2;
        END IF;

        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;

--changeset judo:postgresql-create-aggregate-min-uuid dbms:postgresql  logicalFilePath:postgresql-create-aggregate-min-uuid stripComments:true splitStatements:false failOnError:true
    create aggregate min(uuid) (
      sfunc = min_uuid,
      stype = uuid,
      combinefunc = min_uuid,
      parallel = safe,
      sortop = operator (<)
    );

--changeset judo:postgresql-create-max-uuid dbms:postgresql  logicalFilePath:postgresql-create-max-uuid stripComments:true splitStatements:false failOnError:true
   CREATE FUNCTION max_uuid(uuid, uuid)
    RETURNS uuid AS $$
    BEGIN
        -- if they're both null, return null
        IF $2 IS NULL AND $1 IS NULL THEN
            RETURN NULL ;
        END IF;

        -- if just 1 is null, return the other
        IF $2 IS NULL THEN
            RETURN $1;
        END IF ;
        IF $1 IS NULL THEN
            RETURN $2;
          END IF;

        -- neither are null, return the larger one
        IF $1 < $2 THEN
            RETURN $1;
        END IF;

        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;

--changeset judo:postgresql-create-aggregate-max-uuid dbms:postgresql  logicalFilePath:postgresql-create-aggregate-max-uuid stripComments:true splitStatements:false failOnError:true
    create aggregate max(uuid) (
      sfunc = max_uuid,
      stype = uuid,
      combinefunc = max_uuid,
      parallel = safe,
      sortop = operator (>)
    );

