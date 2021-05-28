INSERT
  INTO security_groups
  (
    id,
    name,
    title,
    is_hidden,
    tech$load_id
  )
VALUES
  (
    :id,
    :name,
    :title,
    :is_hidden,
    :tech$load_id
  )
