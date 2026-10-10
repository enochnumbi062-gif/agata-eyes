# AgAtA-EyEs

**Agrégation de données spatiales · provenance · visualisation 3D · assistant IA (Gemini)**

AgAtA-EyEs agrège le catalogue satellite de CelesTrak et les vecteurs d'état de NASA/JPL Horizons, les expose via une API REST documentée, les stocke dans **Supabase** et les affiche dans un globe 3D à **palettes flottantes**. Un assistant **Gemini** commente les données affichées.

> **Limite d'usage :** démonstrateur, pas un service certifié de surveillance de l'espace ni d'évitement de collision. Les positions viennent d'une propagation SGP4 d'éléments moyens : la précision diminue avec l'âge des éléments.

## Fonctionnalités

- Globe plein écran (Cesium) ; six palettes flottantes déplaçables, redimensionnables, réductibles, positions mémorisées (touches `1`–`6`, `H` = mode immersif).
- Catalogue CelesTrak (stations, actifs, visuels, météo, GPS, GEO) avec propagation **SGP4** en direct (satellite.js), trajectoire sur une période, temps simulé ×60 / ×600.
- Éphémérides NASA/JPL Horizons (allowlist stricte) avec vecteurs d'état analysés côté serveur.
- Provenance : source, horodatage UTC, empreinte SHA-256, cache périmé signalé (`stale`).
- **Supabase** : cache durable (`agata_cache`) et journal d'audit (`agata_audit`) ; repli en mémoire si Supabase est absent.
- **Gemini** : `POST /api/v1/ai/analyze` avec contexte (objet sélectionné, catalogue, éphéméride). Limites par IP et quota quotidien global.

## Mise en place

### 1. Supabase
1. Créez un projet sur https://supabase.com.
2. **SQL Editor → New query** : collez `supabase_schema.sql` puis **Run**.
3. **Project Settings → API** : notez l'URL du projet et la clé `service_role` (ou clé secrète `sb_secret_…`). Cette clé reste côté serveur uniquement.

### 2. Gemini
Créez une clé sur https://aistudio.google.com/apikey. Les modèles essayés dans l'ordre sont dans `GEMINI_MODELS` (le suivant est utilisé si un modèle renvoie 404 pour votre clé).

### 3. GitHub
```bash
git init && git add . && git commit -m "AgAtA-EyEs 2.0"
git branch -M main
git remote add origin https://github.com/VOTRE-COMPTE/agata-eyes.git
git push -u origin main
```
Ne commitez jamais `.env`, une clé ou un mot de passe.

### 4. Render
**New + → Blueprint** → choisissez le dépôt → Render lit `render.yaml`. Saisissez les trois secrets demandés : `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `GEMINI_API_KEY`. Vérifiez ensuite `/health`, `/ready` (doit répondre `"database":"supabase"`), `/docs`.

### En local
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
set -a; source .env; set +a      # .env n'est pas chargé automatiquement
uvicorn main:app --reload
```
Sans variables Supabase/Gemini, l'application fonctionne (cache en mémoire, IA désactivée).

## API

| Endpoint | Rôle |
|---|---|
| `GET /health`, `GET /ready` | Sonde de vie / base joignable |
| `GET /api/v1/status` | Fournisseurs, cache, mode de stockage, état de l'IA |
| `GET /api/v1/satellites?group=stations` | Groupe du catalogue (max `MAX_OBJECTS`) |
| `GET /api/v1/satellites/{norad_id}` | Un objet, requête CelesTrak directe |
| `GET /api/v1/jpl/vectors?target=earth&start=2026-10-09&stop=2026-10-10&step=1%20d` | Vecteurs JPL (+ `vectors` analysés) |
| `POST /api/v1/ai/analyze` | `{"question": "...", "context": {...}}` → réponse Gemini |
| `GET /api/v1/audit?limit=20` | Derniers événements d'audit |

## Sécurité et limites

- Clés Supabase et Gemini uniquement côté serveur ; RLS activée sans policy sur les tables.
- CORS limité à la même origine par défaut (`ALLOWED_ORIGINS`). Les questions posées à l'IA ne sont pas stockées (seule leur empreinte l'est).
- Limites de débit en mémoire, par instance : pour plusieurs instances, utilisez une passerelle ou un limiteur partagé.
- SHA-256 prouve l'intégrité après réception, pas la véracité de la source. Aucune authentification utilisateur : n'ajoutez pas de données privées sans contrôle d'accès.
- Plan Render gratuit : mise en veille après inactivité ; le cache Supabase évite de tout recharger.

Sources : [CelesTrak](https://celestrak.org/) · [NASA/JPL Horizons](https://ssd.jpl.nasa.gov/horizons/) · [NASA GIBS](https://earthdata.nasa.gov/gibs) · [Supabase](https://supabase.com/docs) · [Gemini API](https://ai.google.dev/gemini-api/docs)
