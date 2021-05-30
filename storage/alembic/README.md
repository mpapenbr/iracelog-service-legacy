# Configuration

The data connection string has to be configured via environment variable `DB_URL`.  
Example: 
```
DB_URL="postgresql://dbuser:dbpassword@dbhost:dbport/dbschema"
```

# Detect changes

```
alembic revision --autogenerate -m "Enter text here"
```

This will generate a new file with the detected changes in `schema.py`. Make sure to verify the generated file.

# Apply changes

```
alembic upgrade head
```


# Temporary use a different DB

```
env DB_URL=.... alembic upgrade head
```
This will set the environment var `DB_URL` just for the next call (which is alembic). After returning from that call the value of `DB_URL` in the current shell is unchanged.
