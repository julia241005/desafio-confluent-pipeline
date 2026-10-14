-- Gera um evento de cada tipo no tópico desafio.public.transactions

-- c (create)
INSERT INTO transactions (card_id, account_id, merchant_id, amount)
VALUES (1, 1, 1, 123.45);

-- u (update)
UPDATE transactions
SET status = 'CHARGEBACK'
WHERE transaction_id = (SELECT max(transaction_id) FROM transactions);

-- d (delete) + tombstone
DELETE FROM transactions
WHERE transaction_id = (SELECT max(transaction_id) FROM transactions);
