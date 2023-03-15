---
-- #%L
-- JUDO Runtime Core :: RDBMS Data Access Objects PostgreSQL adapter
-- %%
-- Copyright (C) 2018 - 2023 BlackBelt Technology
-- %%
-- This program and the accompanying materials are made available under the
-- terms of the Eclipse Public License 2.0 which is available at
-- http://www.eclipse.org/legal/epl-2.0.
-- 
-- This Source Code may also be made available under the following Secondary
-- Licenses when the conditions for such availability set forth in the Eclipse
-- Public License, v. 2.0 are satisfied: GNU General Public License, version 2
-- with the GNU Classpath Exception which is
-- available at https://www.gnu.org/software/classpath/license.html.
-- 
-- SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
-- #L%
---
--liquibase formatted sql
--changeset judo:postgresql-create-function-min-uuid dbms:postgresql  logicalFilePath:postgresql-create-function-min-uuid stripComments:true splitStatements:false failOnError:true
   drop function if exists min_uuid(uuid, uuid) cascade;
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
    drop function if exists min(uuid) cascade;
    create aggregate min(uuid) (
      sfunc = min_uuid,
      stype = uuid,
      combinefunc = min_uuid,
      parallel = safe,
      sortop = operator (<)
    );

--changeset judo:postgresql-create-max-uuid dbms:postgresql  logicalFilePath:postgresql-create-max-uuid stripComments:true splitStatements:false failOnError:true
   drop function if exists max_uuid(uuid, uuid) cascade;
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
   drop function if exists max(uuid) cascade;
    create aggregate max(uuid) (
      sfunc = max_uuid,
      stype = uuid,
      combinefunc = max_uuid,
      parallel = safe,
      sortop = operator (>)
    );

