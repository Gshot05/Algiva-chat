CREATE TABLE IF NOT EXISTS messages
(
    chat_id    BIGINT                                 NOT NULL,
    bucket     INTEGER                                NOT NULL,
    message_id BIGINT                                 NOT NULL,
    sender_id  VARCHAR(45)                            NOT NULL,
    content    TEXT                                   NOT NULL,
    sent_at    TIMESTAMPTZ default now()              NOT NULL,
    shard_key  BIGINT, -- GENERATED ALWAYS AS (hashtext(chat_id || ':' || bucket)) STORED,
    is_deleted BOOL DEFAULT FALSE NOT NULL,
    is_read BOOL DEFAULT FALSE NOT NULL,
    edit TIMESTAMPTZ NULL,
    CONSTRAINT pk_messages  PRIMARY KEY (shard_key, message_id)
);

CREATE TABLE IF NOT EXISTS chat_message_counters (
    chat_id BIGINT PRIMARY KEY,
    last_message_id BIGINT DEFAULT 1 NOT NULL,
    bucket INTEGER DEFAULT 1 NOT NULL
);

CREATE TABLE IF NOT EXISTS chats (
    chat_id bigserial NOT NULL,
    service varchar(45) NOT NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    chat_name varchar(128) NOT NULL,
    active bool DEFAULT true NOT NULL,
    metadata JSONB NOT NULL,
    CONSTRAINT pk_chats PRIMARY KEY (chat_id)
);

--
CREATE TABLE IF NOT EXISTS chat_member_types (
    member_type_id INTEGER,
    name VARCHAR,
    CONSTRAINT pk_chat_member_types PRIMARY KEY (member_type_id)
);

CREATE TABLE IF NOT EXISTS chat_members (
   member_id VARCHAR(45),
    chat_id BIGINT,
    member_type_id INTEGER,
    CONSTRAINT pk_chat_members PRIMARY KEY (member_id, chat_id)
);

CREATE OR REPLACE PROCEDURE edit_message(
    IN p_chat_id BIGINT,
    IN p_sender_id VARCHAR,
    IN p_message_id BIGINT,
    IN new_content TEXT
)
LANGUAGE plpgsql
AS
$$
DECLARE
v_bucket_id INTEGER;
    v_bucket_size CONSTANT INTEGER := 10000;
    computed_shard_key BIGINT;
    in_strict INTEGER;
BEGIN
    v_bucket_id := FLOOR((p_message_id - 1) / v_bucket_size) + 1;
    -- Вычисление shard_key
    computed_shard_key := (p_chat_id << 32) | v_bucket_id;

    --BEGIN
    UPDATE messages
    SET
        content = new_content,
        edit = NOW()
    WHERE
        shard_key = computed_shard_key
        AND message_id = p_message_id
        AND sender_id = p_sender_id
    RETURNING sender_id INTO in_strict;

    IF in_strict IS NULL
            THEN RAISE EXCEPTION 'Невозможно отредактировать не свое или не существующее сообщение';
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE delete_message(
    IN p_chat_id BIGINT,
    IN p_sender_id VARCHAR,
    IN p_message_id BIGINT
)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_bucket_id INTEGER;
    v_bucket_size CONSTANT INTEGER := 10000;
    computed_shard_key BIGINT;
    in_strict INTEGER;
BEGIN
    v_bucket_id := FLOOR((p_message_id - 1) / v_bucket_size) + 1;
    -- Вычисление shard_key
    computed_shard_key := (p_chat_id << 32) | v_bucket_id;

    UPDATE messages
    SET
        is_deleted = TRUE
    WHERE
        shard_key = computed_shard_key
        AND message_id = p_message_id
        AND sender_id = p_sender_id
    RETURNING sender_id INTO in_strict;

    IF in_strict IS NULL
            THEN RAISE EXCEPTION 'Невозможно удалить не свое или не существующее сообщение';
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE watch_message(
    IN p_chat_id BIGINT,
    IN p_sender_id VARCHAR,
    IN p_message_id BIGINT
)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_bucket_id INTEGER;
    v_bucket_size CONSTANT INTEGER := 10000;
    computed_shard_key BIGINT;
    in_strict varchar;
BEGIN
    v_bucket_id := FLOOR((p_message_id - 1) / v_bucket_size) + 1;
    -- Вычисление shard_key
    computed_shard_key := (p_chat_id << 32) | v_bucket_id;

    UPDATE messages
    SET
        is_read = TRUE
    WHERE
        shard_key = computed_shard_key
      AND message_id = p_message_id
      AND sender_id <> p_sender_id
        RETURNING sender_id INTO in_strict;

    IF in_strict IS NULL THEN
            RAISE EXCEPTION 'Невозможно прочитать свое или не существующее сообщение';
    END IF;
END;
$$;


CREATE OR REPLACE PROCEDURE add_message(
    IN p_chat_id BIGINT,
    IN p_sender_id VARCHAR,
    IN p_content TEXT,
    IN p_attach_path TEXT
)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_message_id BIGINT;
    v_bucket     INTEGER;
    v_bucket_size CONSTANT INTEGER := 10000;
    v_attachment TEXT;
    v_attachment_id BIGINT;
    v_shard_key BIGINT;
BEGIN
    -- Инициализация или обновление счётчика сообщений
    BEGIN
        -- Попытка получить текущий last_message_id
        SELECT cm.last_message_id
        INTO STRICT v_message_id
        FROM chat_message_counters cm
        WHERE cm.chat_id = p_chat_id
        FOR UPDATE;

        v_message_id := v_message_id + 1;
        v_bucket := FLOOR((v_message_id - 1) / v_bucket_size) + 1;

                -- Обновление счётчика
        UPDATE chat_message_counters
        SET last_message_id = v_message_id, bucket = v_bucket
        WHERE chat_id = p_chat_id;

    EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Если счётчика нет — создаём первую запись
        v_message_id := 1;
        v_bucket := 1;

        INSERT INTO chat_message_counters(chat_id, last_message_id, bucket)
        VALUES (p_chat_id, v_message_id, v_bucket);
    END;

    -- Добавление участника чата
    INSERT INTO chat_members (chat_id, member_id)
    VALUES (p_chat_id, p_sender_id)
    ON CONFLICT (member_id, chat_id) DO NOTHING;

    v_shard_key := ((p_chat_id << 32) | v_bucket);

    INSERT INTO messages (chat_id, bucket, message_id, sender_id, content, shard_key)
    VALUES (p_chat_id, v_bucket, v_message_id, p_sender_id, p_content, v_shard_key);

    IF p_attach_path IS NOT NULL AND p_attach_path <> '' THEN
            -- Split the path by commas and insert into attachment table
            FOREACH v_attachment IN ARRAY string_to_array(p_attach_path, ',') LOOP
                INSERT INTO attachment (attachment_path)
                VALUES (v_attachment)
                RETURNING attachment_id INTO v_attachment_id;

                INSERT INTO message_attachments (shard_key, message_id, attachment_id)
                VALUES (v_shard_key, v_message_id, v_attachment_id);
            END LOOP;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION get_paginated_messages_backward(
    p_chat_id BIGINT,
    p_last_loaded_message_id BIGINT DEFAULT NULL,
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE(chat_id BIGINT, bucket INTEGER, message_id BIGINT, sender_id VARCHAR, content TEXT, sent_at TIMESTAMPTZ, is_read bool, is_deleted bool, edit TIMESTAMPTZ, attachments TEXT)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_bucket_id INTEGER;
    v_msg_id BIGINT;
    v_bucket_size CONSTANT INTEGER := 10000;
BEGIN
    -- Определение последнего сообщения
    IF p_last_loaded_message_id IS NULL THEN
        SELECT cm.last_message_id
        INTO v_msg_id
        FROM chat_message_counters cm
        WHERE cm.chat_id = p_chat_id
        LIMIT 1;

        v_msg_id:= v_msg_id + 1;
    ELSE
        v_msg_id := p_last_loaded_message_id;
    END IF;
        IF v_msg_id IS NULL THEN
            RETURN;
    END IF;

    -- Определение bucket_id
    v_bucket_id := FLOOR((v_msg_id - 1) / v_bucket_size) + 1;

    -- Возврат пагинированных сообщений
    RETURN QUERY
    WITH attach_cte AS (
        SELECT ma.shard_key, ma.message_id, string_agg(a.attachment_path,',') as "attachments" FROM message_attachments ma
        JOIN attachment a ON ma.attachment_id = a.attachment_id
        GROUP BY ma.shard_key, ma.message_id
    )
    SELECT
        s.chat_id, s.bucket, s.message_id, s.sender_id, s.content, s.sent_at, s.is_read, s.is_deleted, s.edit, COALESCE(a."attachments",'')
    FROM messages s
    LEFT JOIN attach_cte a ON (a.shard_key = s.shard_key AND a.message_id = s.message_id)
    WHERE
        (
            s.shard_key = (p_chat_id << 32) | v_bucket_id
            OR s.shard_key = (p_chat_id << 32) | (v_bucket_id - 1)
        )
        AND s.is_deleted = FALSE
        AND s.message_id < v_msg_id
    ORDER BY s.bucket DESC, s.message_id DESC
    LIMIT p_limit;
END;
$$;

CREATE OR REPLACE FUNCTION get_paginated_messages(
    p_chat_id BIGINT,
    p_last_loaded_message_id BIGINT DEFAULT NULL,
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE(chat_id BIGINT, bucket INTEGER, message_id BIGINT, sender_id VARCHAR, content TEXT, sent_at TIMESTAMPTZ, is_read bool, is_deleted bool, edit TIMESTAMPTZ, attachments TEXT)
LANGUAGE plpgsql
AS
$$
DECLARE
    v_bucket_id INTEGER;
    v_msg_id BIGINT;
    v_bucket_size CONSTANT INTEGER := 10000;
BEGIN
    -- Определение последнего сообщения
    IF p_last_loaded_message_id IS NULL THEN
        SELECT cm.last_message_id
        INTO v_msg_id
        FROM chat_message_counters cm
        WHERE cm.chat_id = p_chat_id
        LIMIT 1;
    ELSE
        v_msg_id := p_last_loaded_message_id;
    END IF;

    IF v_msg_id IS NULL THEN
        RETURN;
    END IF;

    -- Определение bucket_id
    v_bucket_id := FLOOR((v_msg_id - 1) / v_bucket_size) + 1;

        -- Возврат пагинированных сообщений
    RETURN QUERY
    WITH attach_cte AS (
        SELECT ma.shard_key, ma.message_id, string_agg(a.attachment_path,',') as "attachments" FROM message_attachments ma
        JOIN attachment a ON ma.attachment_id = a.attachment_id
        GROUP BY ma.shard_key, ma.message_id
    )
    SELECT * FROM (
      SELECT
          s.chat_id, s.bucket, s.message_id, s.sender_id, s.content, s.sent_at, s.is_read, s.is_deleted, s.edit, COALESCE(a."attachments",'')
      FROM messages s
       LEFT JOIN attach_cte a ON (a.shard_key = s.shard_key AND a.message_id = s.message_id)
      WHERE
      (
        s.shard_key = (p_chat_id << 32) | v_bucket_id
        OR s.shard_key = (p_chat_id << 32) | (v_bucket_id + 1)
      )
      AND s.is_deleted = FALSE
      AND s.message_id > v_msg_id
      ORDER BY s.bucket ASC, s.message_id ASC
      LIMIT p_limit
    ) AS t
    ORDER BY bucket DESC, message_id DESC;
END;
$$;


CREATE TABLE IF NOT EXISTS attachment (
  attachment_id   BIGSERIAL,
  attachment_path VARCHAR,
  rev timestamptz DEFAULT now() NOT NULL,
  CONSTRAINT attachment_pk PRIMARY KEY(attachment_id)
);


-- Можно получить message_id по shard_key и message_id
CREATE TABLE IF NOT EXISTS message_attachments (
   shard_key  BIGINT NOT NULL,
   message_id BIGINT NOT NULL,
   attachment_id BIGINT NOT NULL,
   CONSTRAINT pk_message_attachments
   PRIMARY KEY  (shard_key,message_id,attachment_id)
);

CREATE OR REPLACE FUNCTION add_message_return_id(p_chat_id bigint, p_sender_id character varying, p_content text, p_attach_path text)
 RETURNS TABLE(last_message_id bigint)
 LANGUAGE plpgsql
AS $function$
BEGIN
CALL add_message(p_chat_id,p_sender_id,p_content,p_attach_path);

RETURN QUERY SELECT cmc.last_message_id FROM chat_message_counters cmc WHERE chat_id=p_chat_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION get_paginated_messages_backward_sort(p_chat_id bigint, p_last_loaded_message_id bigint DEFAULT NULL::bigint, p_limit integer DEFAULT 50)
 RETURNS TABLE(chat_id bigint, bucket integer, message_id bigint, sender_id character varying, content text, sent_at timestamp with time zone, is_read boolean, is_deleted boolean, edit timestamp with time zone, attachments text)
 LANGUAGE plpgsql
AS $function$
DECLARE
v_bucket_id INTEGER;
    v_msg_id BIGINT;
    v_bucket_size CONSTANT INTEGER := 10000;
BEGIN
    -- Определение последнего сообщения
    IF p_last_loaded_message_id IS NULL THEN
SELECT cm.last_message_id
INTO v_msg_id
FROM chat_message_counters cm
WHERE cm.chat_id = p_chat_id
    LIMIT 1;
v_msg_id:= v_msg_id + 1;
ELSE
        v_msg_id := p_last_loaded_message_id;
END IF;

    IF v_msg_id IS NULL THEN
        RETURN;
END IF;

    -- Определение bucket_id
    v_bucket_id := FLOOR((v_msg_id - 1) / v_bucket_size) + 1;

    -- Возврат пагинированных сообщений
RETURN QUERY
    WITH attach_cte AS (
        SELECT ma.shard_key, ma.message_id, string_agg(a.attachment_path,',') as "attachments" FROM message_attachments ma
        JOIN attachment a ON ma.attachment_id = a.attachment_id
        GROUP BY ma.shard_key, ma.message_id
    )
SELECT s.chat_id, s.bucket, s.message_id, s.sender_id, s.content, s.sent_at, s.is_read, s.is_deleted, s.edit, COALESCE(a."attachments",'')
FROM messages s
         LEFT JOIN attach_cte a ON (a.shard_key = s.shard_key AND a.message_id = s.message_id)
WHERE
    (
        s.shard_key = (p_chat_id << 32) | v_bucket_id
            OR s.shard_key = (p_chat_id << 32) | (v_bucket_id - 1)
        )
  AND s.is_deleted = FALSE
  AND s.message_id < v_msg_id
ORDER BY s.bucket ASC, s.message_id ASC
    LIMIT p_limit;
END;
$function$
;

CREATE OR REPLACE FUNCTION assign_ticket_executor(_chat_id bigint, _status_uid text, _member_id text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
UPDATE chats
SET metadata = JSONB_SET(metadata, ARRAY['status_uid'], TO_JSONB(_status_uid::TEXT))
WHERE chat_id = _chat_id;

DELETE FROM chat_members
WHERE chat_id = _chat_id AND member_type_id = 1;

INSERT INTO chat_members(chat_id, member_id, member_type_id)
VALUES (_chat_id, _member_id, 1);
END;
$function$
;