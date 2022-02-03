from psycopg2 import sql

# Функции sql запросов возвращают SQL объекты с расставленными
# в необходимых местах именными placeholder'ами
def fw_full_sql_query() -> sql.SQL:
    return sql.SQL(
        """
        SELECT
            fw.id as fw_id,
            fw.rating as imdb_rating,
            fw.title,
            fw.description,
            fw.updated_at,
            JSON_AGG(DISTINCT jsonb_build_object('id', g.id, 'name', g.name)) AS "genres",
            ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'actor') AS "actors_names",
            ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'writer') AS "writers_names",
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers,
            JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'director') AS directors
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.updated_at > {updated_at}
        GROUP BY fw_id, fw.updated_at
        ORDER BY fw.updated_at
        LIMIT {sql_limit};
        """
    ).format(
        updated_at=sql.Placeholder(name="updated_at"),
        sql_limit=sql.Placeholder(name="sql_limit"),
    )


def fw_persons_sql_query() -> sql.SQL:
    return sql.SQL(
        """
    SELECT
        fw.id as fw_id,
        ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'director') AS "director",
        ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'actor') AS "actors_names",
        ARRAY_AGG(DISTINCT p."full_name" ) FILTER (WHERE pfw."role" = 'writer') AS "writers_names",
        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
        JSON_AGG(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE fw.id IN {filmwork_ids}
    GROUP BY fw_id;
    """
    ).format(filmwork_ids=(sql.Placeholder(name="filmwork_ids")))


def fw_genres_sql_query() -> sql.SQL:
    return sql.SQL(
        """
        SELECT
            fw.id as fw_id,
            ARRAY_AGG(DISTINCT g.name ) AS "genres"
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN {filmwork_ids}
        GROUP BY fw_id;
        """
    ).format(filmwork_ids=(sql.Placeholder(name="filmwork_ids")))


def nested_pre_sql(table: str) -> sql.SQL:
    return sql.SQL(
        """
        SELECT id, updated_at
        FROM content.{table}
        WHERE updated_at > {updated_at}
        ORDER BY updated_at
        LIMIT {limit};
    """
    ).format(
        table=sql.Identifier(table),
        updated_at=sql.Placeholder(name="updated_at"),
        limit=sql.Placeholder(name="limit"),
    )


def nested_fw_ids_sql(related_table: str, related_id: str) -> sql.SQL:
    return sql.SQL(
        """
    SELECT fw.id, fw.updated_at
    FROM content.film_work fw
    LEFT JOIN content.{related_table} rfw ON rfw.film_work_id = fw.id
    WHERE rfw.{related_id} IN {data_name_ids}
    ORDER BY fw.updated_at
    LIMIT {limit}
    OFFSET {offset}
    """
    ).format(
        related_table=sql.Identifier(related_table),
        related_id=sql.Identifier(related_id),
        data_name_ids=sql.Placeholder(name="data_ids"),
        offset=sql.Placeholder(name="offset"),
        limit=sql.Placeholder(name="limit"),
    )

def person_sql() -> sql.SQL:
    return sql.SQL(
        """
        SELECT p.id,
               p.full_name,
               p.updated_at,
               JSON_AGG(DISTINCT jsonb_build_object('id', pfw.film_work_id, 'role', pfw.role) ) as "role",
               ARRAY_AGG(DISTINCT pfw.film_work_id ) AS "film_ids"
        FROM content.person p
        LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
        WHERE p.updated_at > {updated_at}
        GROUP BY p.id, p.updated_at
        ORDER BY updated_at
        LIMIT {limit}
        """
    ).format(
        updated_at=sql.Placeholder(name="updated_at"),
        limit=sql.Placeholder(name="limit")
    )

def genre_sql() -> sql.SQL:
    return sql.SQL(
        """
        SELECT id,
               name,
               updated_at
        FROM content.genre
        WHERE updated_at > {updated_at}
        ORDER BY updated_at
        LIMIT {limit}
        """
    ).format(
        updated_at=sql.Placeholder(name="updated_at"),
        limit=sql.Placeholder(name="limit")
    )