
# **Distributed File Orchestration and Synchronization**  
A **multi-node data transfer framework for Linux** leveraging **Apache Kafka** to enable seamless distributed file operations. This system provides secure file uploads, downloads, and management across distributed nodes with built-in authentication.  

---

## **Project Overview**  
This project implements a distributed file storage system with the following key features:  
- **User Authentication**  
- **File Operations**: Upload, Download, Delete, and Listing  
- **Multi-Client Support**  
- **Asynchronous File Handling** using Kafka  

---

## **System Architecture**  
The system comprises three primary components:  
1. **Kafka Server**: Responsible for file storage, management, and request handling.  
2. **Kafka Client**: Acts as the user interface for file operations.  
3. **Kafka Configuration**: A centralized module for shared configuration settings.  

### **Directory Structure**  
```
project_root/  
├── kafka_server.py        # Server implementation  
├── kafka_client.py        # Client implementation  
├── kafka_config.py        # Shared configuration  
├── id_passwd.txt          # User credentials  
└── server_storage/        # Server-side file storage  
    └── {username}/        # User-specific directories  
```

---

## **Prerequisites**  
Ensure the following are installed:  
- **Python 3.7+**  
- **Apache Kafka**  
- Python Libraries:  
  - `kafka-python`  
  - `json`  
  - `base64`  

---

## **Installation**  

For installation of kafka, follow the steps in the following link:
``` 

```  

## **Usage**  

### **Step 1: Start the Kafka File Server**  
Run the server to handle file requests:  
```bash  
python kafka_server.py  
```  

### **Step 2: Run the Kafka File Client**  
Launch the client to perform file operations:  
```bash  
python kafka_client.py  
```  

### **Available Commands**  
| **Command**       | **Description**                            |  
|--------------------|--------------------------------------------|  
| `UPLOAD filename`  | Uploads a file to the server               |  
| `DOWNLOAD filename`| Downloads a file from the server           |  
| `DELETE filename`  | Deletes a file from the server             |  
| `VIEW`             | Lists all files in the user's storage      |  
| `EXIT`             | Exits the client application              |  

---

## **Authentication**  
User credentials are stored in `id_passwd.txt` in the format:  
```
username:password  
```  

### **Test Credentials**  
- **Username**: User1, **Password**: 1234  
- **Username**: User2, **Password**: 6789  

---

## **Implementation Details**  

### **Server Component**  
The server (`kafka_server.py`) handles:  
- Authentication  
- File storage and directory management  
- Multi-threaded processing of requests  
- File operations (upload, download, delete, view)  

### **Client Component**  
The client (`kafka_client.py`) provides:  
- An intuitive user interface for file operations  
- Authentication validation  
- File transfer functionalities  

### **Configuration Module**  
The shared configuration (`kafka_config.py`) includes:  
- Kafka broker settings  
- Topic configuration  
- Utilities for Kafka producer/consumer creation  

---

## **Security Considerations**  
- **Password Protection**: Currently transmitted as plain text; should be enhanced for production.  
- **File Encoding**: Base64 encoding during transfers.  
- **User Segregation**: User-specific storage directories ensure isolation.  
- **Authentication**: Required for all file operations.  

---

## **Future Enhancements**  
1. **Secure Password Hashing**: Use robust algorithms like bcrypt.  
2. **File Encryption**: Ensure secure transfers with encryption protocols.  
3. **File Chunking**: Support large file transfers in chunks.  
4. **Versioning**: Maintain multiple versions of uploaded files.  
5. **Real-Time Synchronization**: Enable automatic file synchronization across nodes.  
6. **File Sharing**: Allow inter-user file sharing.  

---

## **Error Handling**  
The system handles the following errors gracefully:  
- File not found  
- Authentication failures  
- Network issues  
- Invalid commands  
- Insufficient storage permissions  

---

## **Contributing**  
We welcome contributions! Feel free to submit issues, enhancement requests, or code contributions.  

For queries, contact:  
📧 **shaheedrm678@gmail.com**  
