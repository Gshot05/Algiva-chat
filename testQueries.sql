-- Добавляем тип "Покупатель" 
INSERT INTO chat_member_types (member_type_id, name) 
VALUES (2, 'buyer') 
ON CONFLICT (member_type_id) DO NOTHING;

-- Добавляем тип "Продавец" 
INSERT INTO chat_member_types (member_type_id, name) 
VALUES (1, 'seller') 
ON CONFLICT (member_type_id) DO NOTHING;
 
Select * from chat_member_types


-- Создаем чат
INSERT INTO chats (chat_name, service, active, metadata)
VALUES (
    'Чат по лоту LOT-999', 
    'ads', 
    true, 
    '{"lot_id": "LOT-999", "uid_buyer": "user_buyer_001", "uid_seller": "user_seller_001", "deal_status": "active"}'::jsonb
)
RETURNING chat_id;

Select * from chats



-- Добавляем продавца (тип 1)
INSERT INTO chat_members (chat_id, member_id, member_type_id)
VALUES (1, 'user_seller_001', 1);

-- Добавляем покупателя (тип 2)
INSERT INTO chat_members (chat_id, member_id, member_type_id)
VALUES (1, 'user_buyer_001', 2);

select * from chat_members



-- Имитация общения
CALL add_message(
    p_chat_id := 1,
    p_sender_id := 'user_buyer_001',
    p_content := 'Здравствуйте! А есть коробка от часов?',
    p_attach_path := NULL
);

CALL add_message(
    p_chat_id := 1,
    p_sender_id := 'user_seller_001',
    p_content := 'Добрый день. Да, полная комплектация с чеком.',
    p_attach_path := NULL
);

CALL add_message(
    p_chat_id := 1,
    p_sender_id := 'user_buyer_001',
    p_content := 'Отлично, тогда беру. Скиньте фото циферблата крупно.',
    p_attach_path := '/uploads/img_001.jpg,/uploads/img_002.jpg'
);

-- Получаем последние 10 сообщений из чата 1
SELECT * FROM get_paginated_messages_backward(
    p_chat_id := 1,
    p_last_loaded_message_id := NULL, -- NULL значит "начни с самого конца"
    p_limit := 10
);


-- Удаляем сообщение с ID 3, которое написал user_buyer_001 в чате 1
CALL delete_message(
    p_chat_id := 1,
    p_sender_id := 'user_buyer_001', -- Обязательно UID автора!
    p_message_id := 3
);


-- Прочитать сообщения
CALL mark_messages_as_read(1, 'user_buyer_001', 3);

-- Получить список своих чатов
WITH profile_chats AS (
    SELECT
        c.chat_id,
        c.service,
        c.created_at,
        c.chat_name,
        c.active,
        c.metadata,
        cm.member_id,
        cm.member_type_id
    FROM chats c
             JOIN chat_members cm ON cm.chat_id = c.chat_id
    WHERE cm.member_id = 'user_buyer_001'  -- $1
      AND c.service = 'ads'                -- $2
      AND (c.active = TRUE OR TRUE IS NULL) -- $3 (упрощено для теста)
),
last_message_info AS (
         SELECT DISTINCT ON (m.chat_id)
             m.chat_id,
             m.content,
             m.sent_at
         FROM profile_chats pc
                  JOIN messages m ON m.chat_id = pc.chat_id
         WHERE m.is_deleted = FALSE
         ORDER BY m.chat_id, m.sent_at DESC
     ),
paginated_chats AS (
         SELECT
             pc.chat_id,
             pc.service,
             pc.created_at,
             pc.chat_name,
             pc.active,
             pc.metadata,
             pc.member_id,
             pc.member_type_id,
             COALESCE(lmi.content, '') AS last_message,
             lmi.sent_at
         FROM profile_chats pc
                  LEFT JOIN last_message_info lmi ON lmi.chat_id = pc.chat_id
         ORDER BY lmi.sent_at DESC NULLS LAST
         LIMIT 20 -- $4
     ),
-- НОВАЯ ЛОГИКА: Считаем непрочитанные через таблицу read_messages
     unread_counts AS (
         SELECT 
            pgc.chat_id, 
            COUNT(m.message_id) AS unread_count
         FROM paginated_chats pgc
         JOIN messages m ON m.chat_id = pgc.chat_id
         WHERE m.sender_id <> 'user_buyer_001' -- $1 (Чужие сообщения)
           AND NOT EXISTS (
               SELECT 1 FROM read_messages rm
               WHERE rm.chat_id = m.chat_id
                 AND rm.message_id = m.message_id
                 AND rm.reader_id = 'user_buyer_001' -- $1 (Которые текущий пользователь еще не прочитал)
           )
         GROUP BY pgc.chat_id
     )
SELECT
    pgc.chat_id,
    pgc.service,
    pgc.created_at,
    pgc.chat_name,
    pgc.active,
    pgc.metadata::TEXT,
    pgc.member_id,
    pgc.member_type_id,
    pgc.last_message,
    COALESCE(uc.unread_count, 0) AS unread_count
FROM paginated_chats pgc
         LEFT JOIN unread_counts uc ON uc.chat_id = pgc.chat_id
ORDER BY pgc.sent_at DESC NULLS LAST;


-- Подаем жалобу от имени покупателя на чат №1
UPDATE chats 
SET metadata = metadata || jsonb_build_object( 
    'has_complaint', true, 
    'complaint_count', COALESCE((metadata->>'complaint_count')::int, 0) + 1, 
    'last_complaint_at', now()::text, 
    'last_complaint_reason', 'contact_exchange', 
    'complaint_message', 'Продавец просит написать в WhatsApp' 
) 
WHERE chat_id = 1;

-- Просмотреть 
SELECT 
    chat_id, 
    metadata->>'has_complaint' as has_complaint,
    metadata->>'complaint_count' as complaint_count,
    metadata->>'last_complaint_reason' as reason,
    metadata->>'complaint_message' as snippet
FROM chats 
WHERE chat_id = 1;

-- Получение чатов для админа
WITH filtered_chats AS (
    SELECT
        c.chat_id,
        c.service,
        c.chat_name,
        c.active,
        c.created_at,
        c.metadata,
        -- Извлекаем важные флаги из метадаты
        COALESCE((c.metadata->>'has_complaint')::boolean, false) as has_complaint,
        COALESCE((c.metadata->>'complaint_count')::int, 0) as complaint_count,
        c.metadata->>'last_complaint_reason' as complaint_reason,
        c.metadata->>'uid_buyer' as uid_buyer,
        c.metadata->>'uid_seller' as uid_seller
    FROM chats c
    WHERE TRUE
      -- Фильтр по сервису
      AND ('' = '' OR c.service = '')
      -- Фильтр по жалобам (сравниваем строки, чтобы избежать ошибок приведения типов)
      AND ('' = '' OR c.metadata->>'has_complaint' = '')
      -- Поиск по имени или метадате
      AND ('' = '' OR LOWER(c.chat_name) LIKE LOWER('%' || '' || '%') OR LOWER(c.metadata::text) LIKE LOWER('%' || '' || '%'))
),
-- Получаем время последнего сообщения для сортировки
last_msg AS (
    SELECT DISTINCT ON (m.chat_id)
        m.chat_id,
        m.sent_at
    FROM messages m
    JOIN filtered_chats fc ON m.chat_id = fc.chat_id
    WHERE m.is_deleted = FALSE
    ORDER BY m.chat_id, m.sent_at DESC
),
-- Пагинация и финальная сортировка
paginated AS (
    SELECT
        fc.*,
        lm.sent_at as last_message_sent_at
    FROM filtered_chats fc
    LEFT JOIN last_msg lm ON lm.chat_id = fc.chat_id
    ORDER BY 
        fc.has_complaint DESC, -- Сначала чаты с жалобами
        lm.sent_at DESC NULLS LAST, -- Затем по времени последнего сообщения
        fc.created_at DESC
    LIMIT 10
    OFFSET 0
)
SELECT
    p.chat_id,
    p.service,
    p.chat_name,
    p.active,
    p.created_at,
    p.last_message_sent_at,
    p.has_complaint,
    p.complaint_count,
    p.complaint_reason,
    p.uid_buyer,
    p.uid_seller,
    p.metadata::TEXT as raw_metadata
FROM paginated p;



-- Используем существующую функцию get_paginated_messages_backward
-- Она автоматически обрабатывает шардирование и подгрузку вложений
-- Её использование описано выше