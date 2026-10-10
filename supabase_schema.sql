-- AgAtA-EyEs — schéma Supabase. Supabase > SQL Editor > New query > coller > Run.
create table if not exists public.agata_cache (
  cache_key      text primary key,
  payload        jsonb not null,
  fetched_at     double precision not null,   -- epoch UTC (secondes)
  source         text not null,
  payload_sha256 text not null,
  updated_at     timestamptz not null default now()
);

create table if not exists public.agata_audit (
  id             bigint generated always as identity primary key,
  occurred_at    timestamptz not null default now(),
  event_type     text not null,
  source         text not null,
  payload_sha256 text,
  detail         text
);
create index if not exists agata_audit_occurred_idx on public.agata_audit (occurred_at desc);

-- RLS activée sans aucune policy : l'API publique de Supabase ne peut rien lire ni écrire.
-- Seul le backend (clé service_role / secrète, qui contourne la RLS) accède à ces tables.
alter table public.agata_cache enable row level security;
alter table public.agata_audit enable row level security;

-- Optionnel : purge du journal au-delà de 90 jours (à lancer à la main ou via pg_cron).
-- delete from public.agata_audit where occurred_at < now() - interval '90 days';
