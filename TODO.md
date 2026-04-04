# Nexus — TODO

## Architecture globale

```
nexus sync   →  crée KV Bucket [uuid]          (config + ls_done)
             →  crée Stream [uuid-work]        WorkQueuePolicy → uuid.object
             →  crée Stream [uuid-stats]       InterestPolicy  → uuid.statistics
             →  publie KV["config"]            (configuration du transfert)
             →  affiche UUID

nexus ls     →  WalkDir filesystem source
             →  publie uuid.object en streaming avec backpressure
             →  KV["ls_done"] = "true" quand terminé

nexus worker →  pull KV[uuid]["config"]   (configuration du transfert)
             →  pull uuid.object          (fichiers à traiter, work queue)
             →  stat src + dst
             →  transfert si nécessaire
             →  ACK + publish uuid.statistics

nexus status →  subscribe uuid.statistics
             →  affiche progression (objets/sec, bytes, erreurs)
```

---

## Commandes à implémenter

- [ ] `nexus sync`   — initialise le job, retourne un UUID
- [ ] `nexus ls`     — adapater la commande existante pour publier vers NATS avec `--token UUID`
- [ ] `nexus worker` — consommateur de travail distribué
- [ ] `nexus status` — affichage de la progression en temps réel

---

## Infrastructure NATS JetStream

### KV Bucket `[uuid]`

| Clé        | Valeur                          | Rôle                                 |
|------------|---------------------------------|--------------------------------------|
| `config`   | JSON configuration complète     | Lu N fois par les workers            |
| `ls_done`  | `"true"` quand ls a terminé     | Signal de fin pour les workers       |

### Stream `[uuid-work]` — WorkQueuePolicy

| Subject           | Producteur    | Consommateur       |
|-------------------|---------------|--------------------|
| `uuid.object`     | `nexus ls`    | `nexus worker` ×N  |

- Chaque message consommé **une seule fois** par un seul worker
- Message remis à disposition après `AckWait` si le worker crashe (fault tolerance)

### Stream `[uuid-stats]` — InterestPolicy

| Subject             | Producteur      | Consommateur         |
|---------------------|-----------------|----------------------|
| `uuid.statistics`   | `nexus worker`  | `nexus status` ×N    |

- Chaque instance `nexus status` reçoit **tous** les messages
- Lancer `nexus status` avant les workers pour ne rien manquer

---

## Arrêt propre des workers

Les workers ne peuvent pas distinguer "queue vide temporairement" (ls encore en cours)
de "queue vide définitivement" (ls terminé). Mécanisme retenu :

1. **KV watcher** — goroutine dédiée surveille `KV["ls_done"]` en push (pas de polling)
2. **Pull avec timeout** — si timeout + `ls_done = true` → le worker s'arrête

```
nexus ls    →  publie uuid.object...
               publie uuid.object...
               KV["ls_done"] = "true"

workers     →  pull → traite → ACK
               pull → traite → ACK
               pull → timeout → ls_done=false → retry
               pull → timeout → ls_done=true  → EXIT ✓
```

Pourquoi pas de message sentinel EOF : avec N workers et WorkQueue, il faudrait
publier N messages EOF (un par worker). Si un worker crashe, un EOF est perdu.
Le KV watcher est robuste même si un worker redémarre tard.

---

## Serialisation

Les `FileRecord` publiés sur `uuid.object` sont sérialisés en **JSON**.

```json
{
  "path": "/data/file.txt",
  "inode": 12345,
  "type": 8,
  "size": 1024,
  "mtime": 1700000000,
  "ctime": 1700000001
}
```

---

## Notes

- Le token UUID isole les jobs : plusieurs sync peuvent tourner en parallèle
- `nats.go v1.50.0` — compatible NATS Server 2.2+ (testé avec 2.12.6)
- `nexus ls` avec `--token` remplacera `--nats` pour s'intégrer dans ce flux
