# database

Simple postgres wrapper
- Driver: [pgx](https://github.com/jackc/pgx)
- Helper: [sqlx](https://github.com/jmoiron/sqlx)
- Sharding: based on [instagram id generator](https://instagram-engineering.com/sharding-ids-at-instagram-1cf5a71e5a5c)

## Getting started

### Single node
```golang
db, err := database.Connect(context.Background(), "postgres://localhost:5432/mydatabase")
if err != nil {
  panic(err)
}
```

### Cluster
```golang
c, err := database.NewClusterFromFile(context.Background(), "sample.config.yaml")
if err != nil {
	panic(err)
}

err := c.EveryShard(func(shard database.Shard) error {
    _, qErr := shard.S().Exec(`
    create table if not exists accounts (
        id bigint not null default next_id('accounts') primary key,
        name text not null
    )
`)
    return qErr
})
if err != nil {
    panic(err)
}

shard := c.NextShard()
var id uint64
shard.S().QueryRow(`insert into accounts (name) values ($1) returning id`, "John").Scan(&id)
fmt.Println(id)

shard = c.ShardByKey(id)
if shard != nil {
	var name string
	shard.S().QueryRow(`select name from accounts where id = $1`, id).Scan(&name)
	fmt.Println(name)
}


```