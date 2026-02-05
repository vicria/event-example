## 🐳 MongoDB (Local Development)

This project uses **MongoDB Change Streams**, so MongoDB must run as a **replica set**.  
Otherwise, you will get the error:

> The `$changeStream` stage is only supported on replica sets

---

### 🚀 Start MongoDB with Docker

Run MongoDB using docker-compose:

```bash
docker compose up -d
```
Check that the container is running:
```bash
docker ps
```
Status should be:
> Up ...

### ⚙️ Initialize replica set (required once)
After the first start, you must initialize the replica set one time only.
Open Mongo shell inside the container:
```bash
docker exec -it mongo mongosh
```
Then run:
```bash
rs.initiate()
```
Expected result:
> { ok: 1 }

### 📖 Why this is required
The project uses Change Streams to listen for real-time database updates.
Change Streams only work when MongoDB runs as a replica set, which is why rs.initiate() is required.

###  🔌 Connection
MongoDB will be available at:
mongodb://localhost:27017

### 🛠 Reset everything (if something breaks)
```bash
docker compose down -v
docker compose up -d
docker exec -it mongo mongosh
rs.initiate()
```