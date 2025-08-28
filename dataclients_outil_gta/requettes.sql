SELECT
    pauseagent.id AS id,
    pauseagent.debutnow AS debutnow,
    pauseagent.entre_prod AS entre_prod,
    pauseagent.sortie_prod AS sortie_prod,
    pauseagent.pausebrief AS pausebrief,
    pauseagent.arrete_pausebrief AS arrete_pausebrief,
    pauseagent.pausedej AS pausedej,
    pauseagent.arrete_pausedej AS arrete_pausedej,
    pauseagent.pauseperso AS pauseperso,
    pauseagent.arrete_pauseperso AS arrete_pauseperso,
    pauseagent.pauseformation AS pauseformation,
    pauseagent.arrete_pauseformation AS arrete_pauseformation,
    pauseagent.pauseautre AS pauseautre,
    pauseagent.arrete_pauseautre AS arrete_pauseautre,
    pauseagent.EnProduction AS EnProduction,
    pauseagent.finjournee AS finjournee,
    MAX(users.name) AS user_name,
    pauseagent.users_id AS users_id,
    pauseagent.actionFinJour AS actionFinJour,
    pauseagent.created_at AS created_at,
    pauseagent.updated_at AS updated_at,
    pauseagent.deleted_at AS deleted_at
FROM
    pauseagent
    LEFT JOIN users ON pauseagent.users_id = users.id
GROUP BY
    pauseagent.id;