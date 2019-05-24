# *REST API Clustering System with Kafka*
## Deskripsi Aplikasi
Aplikasi ini merupakan *clustering engine* yang dibangun menggunakan ``Flask``. Struktur aplikasi utama terdiri dari `server.py`, `app.py`, dan `engine.py` dan menggunakan `Kafka` untuk *data streaming*.
- `server.py` merupakan penyedia layanan dari aplikasi. `server.py` dijalankan pertama kali untuk inisiasi.
- `app.py` dipanggil oleh `server.py` saat proses inisiasi. `app.py` juga menyediakan *routing* untuk aplikasi sekaligus perantara ke `engine.py`
- `engine.py` adalah tempat di mana *logic* atau *backend* aplikasi disimpan. Proses-proses seperti *loading dataset*, *training model*, dan algoritma ``KMeans`` diletakkan di file ini.
## *URL*
*URL* dapat ditambah atau diubah pada file `app.py`. *URL* yang sementara dapat diakses yaitu:
- `http://<host>:<port>/model1/<int:crime_id>/cluster`
- `http://<host>:<port>/model2/<int:crime_id>/cluster`
- `http://<host>:<port>/model3/<int:crime_id>/cluster`

``host`` dan ``port`` dapat dimodifikasi pada file `server.py`. Pada aplikasi kali ini, penulis menggunakan `127.0.0.1` (`localhost`) dengan port `6000`.
## *Testing*
### 1. Mendapatkan Prediksi *Clustering* untuk *INCIDENT_ID* Tertentu pada Model 1
- *URL*: `http://<host>:<port>/model1/<crime_id>/cluster`
- Parameter:
  - ``crime_id`` -> *INCIDENT_ID* yang akan diprediksi
- Metode: `GET`
- Contoh Format: `http://localhost:6000/model1/2018174723/cluster`
- *Output* dan *Screenshot*:

![1](img/model1.PNG)
### 2. Mendapatkan Prediksi *Clustering* untuk *INCIDENT_ID* Tertentu pada Model 2
- *URL*: `http://<host>:<port>/model2/<crime_id>/cluster`
- Parameter:
  - ``crime_id`` -> *INCIDENT_ID* yang akan diprediksi
- Metode: `GET`
- Contoh Format: `http://localhost:6000/model2/20166003240/cluster`
- *Output* dan *Screenshot*:

![2](img/model2.png)
### 3. Mendapatkan Prediksi *Clustering* untuk *INCIDENT_ID* Tertentu pada Model 3
- *URL*: `http://<host>:<port>/model3/<crime_id>/cluster`
- Parameter:
  - ``crime_id`` -> *INCIDENT_ID* yang akan diprediksi
- Metode: `GET`
- Contoh Format: `http://localhost:6000/model3/20176001385/cluster`
- *Output* dan *Screenshot*:

![3](img/model3.PNG)
