# database

A simple database wrapper for Postgres.

### Getting started
```golang
db, err := database.Connect(context.Background(), "postgres://localhost:5432/mydatabase")
if err != nil {
  panic(err)
}
```
