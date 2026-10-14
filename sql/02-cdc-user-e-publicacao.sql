-- O cdc_user foi criado pelo Console do Neon (é o único jeito de dar a role REPLICATION no Neon).
-- A senha NÃO fica neste arquivo.

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON customers, accounts, cards, merchants, transactions TO cdc_user;

CREATE PUBLICATION desafio_pub
  FOR TABLE customers, accounts, cards, merchants, transactions;

-- Conferência
SELECT * FROM pg_publication_tables WHERE pubname = 'desafio_pub';
