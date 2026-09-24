# Student Profile CDC Deployment

This repository contains the Change Data Capture (CDC) infrastructure and Python streaming consumers for the `registrar-cvsu` application. 

It uses **Debezium** to listen to live MySQL database changes, and **FastStream/NATS** to intercept, unify, and distribute these events across your system.

---

## 🛠 Prerequisites
1. **Docker Desktop** (Must be running for Debezium to work).
2. **Python 3.10+** (For running the FastStream consumers).
3. **Running NATS Server** (Configured via `NATS_URL` in your scripts).

To install the necessary Python dependencies for the consumers, navigate to the `consumer` folder and run:
```bash
pip install -r requirements.txt
```

---

## 🚀 1. How to Run Live CDC Streaming (Debezium + FastStream)

If you want to capture live changes (inserts, updates, deletes) on `student_profile` and `student_info` and publish them as a single, unified JSON payload to the `academics.enrollment.main.student.profile` stream, follow these steps:

### Step 1: Start Debezium (Docker Required)
Open Docker Desktop on your Windows machine, open a standard PowerShell, and run:
```powershell
cd c:\laragon\www\student-profile-cdc-deployment
docker compose up -d debezium
```
*Debezium will instantly start watching your local MySQL `registrar-cvsu` database.*

### Step 2: Start the FastStream Consumer
In a new terminal window, navigate to the `consumer` directory and run the FastStream application:
```powershell
cd c:\laragon\www\student-profile-cdc-deployment\consumer
faststream run main:app
```
*This app intercepts the fragmented Debezium events, joins the tables via MySQL, computes `__changed_fields` and `__deleted`, and publishes the live unified payload to your custom NATS stream!*

---

## 🔄 2. How to Mass Sync Legacy Data (Replay)

If you wipe your database (`php artisan migrate:fresh --seed`) and want to **replay** all legacy student data from the `EnrollmentData` stream back into your MySQL database:

```powershell
cd c:\laragon\www\student-profile-cdc-deployment\consumer
python sync_all.py
```
*This script will create a fresh NATS JetStream consumer, fetch all legacy records from the stream, process them, and mass-insert them into the fresh `registrar-cvsu` database.*

---

## 🧹 3. Troubleshooting & Cleanup

When you run `sync_all.py`, it automatically generates a persistent JetStream consumer. If you run it many times during testing, it may leave behind "orphaned" consumers on your NATS server that take up metadata space.

To safely delete all orphaned `mass_sync_consumer` instances from your NATS server, run:
```powershell
cd c:\laragon\www\student-profile-cdc-deployment\consumer
python delete_consumers.py
```
