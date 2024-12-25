import os
from kafka import KafkaConsumer, KafkaProducer
from json import dumps, loads
from base64 import b64decode, b64encode
import threading
from kafka_config import create_producer, create_consumer, FILE_TRANSFER_TOPIC, AUTH_TOPIC

class KafkaFileServer:
    def __init__(self):
        """
        Initialize the KafkaFileServer instance.
        
        This sets up the Kafka producer, prepares the storage directory,
        loads user credentials, and starts the necessary consumers for
        authentication and file transfer.
        """
        # Create a Kafka producer instance
        self.producer = create_producer()
        
        # Set up the server storage directory
        self.storage_dir = os.path.join(os.path.dirname(__file__), "server_storage")
        os.makedirs(self.storage_dir, exist_ok=True)
        
        # Load user credentials from file
        self.credentials = self.load_credentials()
        
        # Start consumers for handling authentication and file transfers
        self.start_auth_consumer()
        self.start_file_transfer_consumer()

    def load_credentials(self):
        """
        Load user credentials from the file "id_passwd.txt"

        This file is expected to be in the same directory as the
        KafkaFileServer Python script. Each line of the file should
        contain a username and password in the format "username:password".

        Returns:
            dict: A dictionary of user credentials, where the keys are
                  usernames and the values are passwords.
        """
        credentials = {}
        credentials_path = os.path.join(os.path.dirname(__file__), "id_passwd.txt")
        with open(credentials_path, "r") as file:
            for line in file:
                # Split the line into username and password
                username, password = line.strip().split(":")
                # Store the credentials in the dictionary
                credentials[username] = password
        return credentials

    def start_auth_consumer(self):
        """
        Start a consumer to handle authentication requests.
        
        This will subscribe to the "authentication" topic and start a
        separate thread to process incoming authentication requests.
        """
        auth_consumer = create_consumer('auth_group')
        auth_consumer.subscribe([AUTH_TOPIC])
        
        def process_auth():
            """
            Process incoming authentication requests.
            
            This function will be called from a separate thread, so it
            will not block the main thread.
            """
            for message in auth_consumer:
                # Extract the authentication data from the message
                data = message.value
                username = data['username']
                password = data['password']
                
                # Check if the username and password are valid
                is_valid = self.credentials.get(username) == password
                
                # Send the authentication result back to the client
                self.producer.send('auth_response', {
                    'client_id': data['client_id'],
                    'success': is_valid,
                    'username': username if is_valid else None
                })
                
                # Log the authentication result
                if is_valid:
                    print(f"Authenticated user {username}")
                else:
                    print(f"Authentication failed for user {username}")
        
        # Start a separate thread to process authentication requests
        threading.Thread(target=process_auth, daemon=True).start()

    def start_file_transfer_consumer(self):
        """
        Start a consumer to handle file transfer requests.
        
        This will subscribe to the "file_transfers" topic and start a
        separate thread to process incoming file transfer requests.
        """
        transfer_consumer = create_consumer('transfer_group')
        transfer_consumer.subscribe([FILE_TRANSFER_TOPIC])
        
        def process_transfers():
            """
            Process incoming file transfer requests.
            
            This function will be called from a separate thread, so it
            will not block the main thread.
            """
            for message in transfer_consumer:
                # Extract the file transfer data from the message
                data = message.value
                action = data['action']
                
                # Handle the file transfer request based on the action
                if action == 'UPLOAD':
                    self.handle_upload(data)
                elif action == 'DOWNLOAD':
                    self.handle_download(data)
                elif action == 'DELETE':
                    self.handle_delete(data)
                elif action == 'VIEW':
                    self.handle_view(data)
                
        # Start a separate thread to process file transfer requests
        threading.Thread(target=process_transfers, daemon=True).start()

    def handle_upload(self, data):
        """
        Handle an upload file transfer request from a client.

        This will save the file to the server storage directory
        and send a response back to the client.

        Args:
            data (dict): The message data containing the file transfer request.
        """
        username = data['username']
        filename = data['filename']
        file_content = b64decode(data['content'])
        
        user_dir = os.path.join(self.storage_dir, username)
        os.makedirs(user_dir, exist_ok=True)
        file_path = os.path.join(user_dir, filename)
        
        print(f"Saving file to: {file_path}")  # Debug log
        
        try:
            with open(file_path, 'wb') as f:
                f.write(file_content)
            
            print(f"File saved successfully. Size: {len(file_content)} bytes")  # Debug log
            
            self.producer.send('transfer_response', {
                'client_id': data['client_id'],
                'success': True,
                'message': 'Upload complete'
            })
        except Exception as e:
            print(f"Error saving file: {str(e)}")  # Debug log
            self.producer.send('transfer_response', {
                'client_id': data['client_id'],
                'success': False,
                'message': f'Upload failed: {str(e)}'
            })

    def handle_download(self, data):
        """
        Handle a download file transfer request from a client.

        This will read the file from the server storage directory
        and send the file content back to the client.

        Args:
            data (dict): The message data containing the file transfer request.

        Raises:
            FileNotFoundError: If the file is not found in the storage directory.
        """
        username = data['username']
        filename = data['filename']
        file_path = os.path.join(self.storage_dir, username, filename)
        
        try:
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    content = f.read()
                
                # Send the file content back to the client
                self.producer.send('transfer_response', {
                    'client_id': data['client_id'],
                    'success': True,
                    'content': b64encode(content).decode(),
                    'filename': filename
                })
            else:
                # Raise an exception if the file is not found
                raise FileNotFoundError(f"File {filename} not found")
        except Exception as e:
            # Send an error response back to the client if an exception occurs
            self.producer.send('transfer_response', {
                'client_id': data['client_id'],
                'success': False,
                'message': str(e)
            })

    def handle_delete(self, data):
        """
        Handle a delete file transfer request from a client.

        This will delete the file from the server storage directory
        and send a response back to the client.

        Args:
            data (dict): The message data containing the file transfer request.

        Raises:
            FileNotFoundError: If the file is not found in the storage directory.
        """
        username = data['username']
        filename = data['filename']
        file_path = os.path.join(self.storage_dir, username, filename)

        try:
            if os.path.exists(file_path):
                # Delete the file
                os.remove(file_path)
                
                # Send a success response back to the client
                self.producer.send('transfer_response', {
                    'client_id': data['client_id'],
                    'success': True,
                    'message': f'File {filename} deleted successfully'
                })
            else:
                # Raise an exception if the file is not found
                raise FileNotFoundError(f"File {filename} not found")
        except Exception as e:
            # Send an error response back to the client if an exception occurs
            self.producer.send('transfer_response', {
                'client_id': data['client_id'],
                'success': False,
                'message': str(e)
            })

    def handle_view(self, data):
        """
        Handle a view file transfer request from a client.

        This will list all files in the server storage directory
        and send a response back to the client.

        Args:
            data (dict): The message data containing the file transfer request.
        """
        username = data['username']
        user_dir = os.path.join(self.storage_dir, username)

        try:
            if os.path.exists(user_dir):
                # List all files in the user's directory
                files = os.listdir(user_dir)
                self.producer.send('transfer_response', {
                    'client_id': data['client_id'],
                    'success': True,
                    'files': files
                })
            else:
                # Return an empty list if the user's directory does not exist
                self.producer.send('transfer_response', {
                    'client_id': data['client_id'],
                    'success': True,
                    'files': []
                })
        except Exception as e:
            # Send an error response back to the client if an exception occurs
            self.producer.send('transfer_response', {
                'client_id': data['client_id'],
                'success': False,
                'message': str(e)
            })


if __name__ == "__main__":
    server = KafkaFileServer()
    print("Kafka File Server is running...")
    # Keep the main thread alive
    threading.Event().wait() 