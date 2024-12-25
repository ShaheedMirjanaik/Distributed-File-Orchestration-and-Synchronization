import os
import uuid
from base64 import b64encode, b64decode
from kafka_config import create_producer, create_consumer, FILE_TRANSFER_TOPIC, AUTH_TOPIC
from kafka.errors import KafkaError

class KafkaFileClient:
    """
    A Kafka client for file transfer and authentication.

    :param client_id: Unique client ID
    :param producer: Kafka producer instance
    :param consumer: Kafka consumer instance
    :param username: Username of the authenticated user
    """

    def __init__(self):
        """
        Initialize the client with a unique client ID, create a producer and a consumer instance.
        """
        self.client_id = str(uuid.uuid4())
        self.producer = create_producer()
        self.consumer = create_consumer(f'client_{self.client_id}')
        self.username = None

    def check_server_connection(self):
        """
        Check if the server is reachable by sending a dummy message.
        
        :return: True if the server is reachable, False otherwise
        """
        try:
            self.producer.send(AUTH_TOPIC, {'client_id': self.client_id, 'check': 'ping'})
            self.producer.flush()
            return True
        except KafkaError:
            print("Server is disconnected. Exiting...")
            return False

    def authenticate(self):
        """
        Authenticate the client with a username and password.

        :return: True if authentication is successful, False otherwise
        """
        if not self.check_server_connection():
            return False

        username = input("Enter username: ")
        password = input("Enter password: ")
        
        self.producer.send(AUTH_TOPIC, {
            'client_id': self.client_id,
            'username': username,
            'password': password
        })
        
        try:
            # Subscribe to auth response
            self.consumer.subscribe(['auth_response'])
            for message in self.consumer:
                data = message.value
                if data['client_id'] == self.client_id:
                    if data['success']:
                        # Store the authenticated username
                        self.username = data['username']
                        print("Authentication successful")
                        return True
                    else:
                        print("Authentication failed")
                        return False
        except KafkaError:
            print("Server is disconnected. Exiting...")
            return False  # Terminate the authentication process

    def send_file_transfer_request(self, action, filename=None, content=None):
        """
        Send a file transfer request to the server based on the given action.

        :param action: The action to perform on the server (UPLOAD/DOWNLOAD/DELETE/VIEW)
        :param filename: The filename to use for the action
        :param content: The file content to send for the UPLOAD action
        """
        if not self.check_server_connection():
            return

        request = {
            'client_id': self.client_id,
            'action': action,
            'username': self.username,
            'filename': filename,
            'content': content
        }
        self.producer.send(FILE_TRANSFER_TOPIC, request)

        try:
            # Subscribe to transfer response and wait for the response
            self.consumer.subscribe(['transfer_response'])
            for message in self.consumer:
                response = message.value
                if response['client_id'] == self.client_id:
                    if response['success']:
                        if action == 'UPLOAD':
                            print("Upload complete")
                        elif action == 'DOWNLOAD':
                            print("Download complete")
                        elif action == 'DELETE':
                            print(response['message'])
                        elif action == 'VIEW':
                            files = response['files']
                            if files:
                                print("\nYour files:")
                                for file in files:
                                    print(f"- {file}")
                            else:
                                print("No files found in your storage")
                    else:
                        if action == 'UPLOAD':
                            print(f"Upload failed: {response['message']}")
                        elif action == 'DOWNLOAD':
                            print(f"Download failed: {response['message']}")
                        elif action == 'DELETE':
                            print(f"Delete failed: {response['message']}")
                        elif action == 'VIEW':
                            print(f"View failed: {response['message']}")
                    break
        except KafkaError:
            print("Server is disconnected. Exiting...")
            return  # Terminate the file transfer request

    def upload_file(self, filepath):
        """
        Upload a file from the local storage to the server.

        :param filepath: The path to the file to upload
        """
        # Convert filepath to correct case if file exists with different case
        directory = os.path.dirname(filepath) or '.'
        filename = os.path.basename(filepath)
        
        # Look for the file with case-insensitive match
        actual_filename = None
        for f in os.listdir(directory):
            if f.lower() == filename.lower():
                actual_filename = f
                break
        
        # Use the actual filename if found, otherwise use original filepath
        final_path = os.path.join(directory, actual_filename) if actual_filename else filepath
        
        if not os.path.exists(final_path):
            print(f"Error: File '{filepath}' not found")
            return
        
        try:
            with open(final_path, 'rb') as f:
                content = f.read()
            
            print(f"Reading file: {final_path}")
            print(f"File size: {len(content)} bytes")
            
            filename = os.path.basename(final_path)
            self.send_file_transfer_request('UPLOAD', filename, b64encode(content).decode())
        except Exception as e:
            print(f"Upload error: {str(e)}")
            
    def download_file(self, filename):
        pass
        
    def delete_file(self, filename):
        pass

    def view_files(self):
        pass

def main():
    """
    Main function to run the Kafka file client for file transfer operations.
    It provides an interactive command-line interface for users to
    authenticate and perform file operations such as UPLOAD, DOWNLOAD,
    DELETE, and VIEW.
    """
    client = KafkaFileClient()
    if not client.authenticate():
        return

    while True:
        # Get the command from the user
        command = input("Enter command (UPLOAD/DOWNLOAD/DELETE/VIEW/EXIT): ").strip()
        command_type = command.split()[0].upper()

        if command_type == "EXIT":
            # Exit the program
            break
        elif command_type == "UPLOAD":
            try:
                # Extract the file path from the command
                filepath = command[7:].strip()
                filepath = filepath.replace(',', '.')
                print(f"Attempting to upload: {filepath}")
                client.upload_file(filepath)
            except IndexError:
                print("Usage: UPLOAD filename.py")
        elif command_type == "DOWNLOAD":
            try:
                # Extract the filename from the command
                filename = command[9:].strip()
                filename = filename.replace(',', '.')
                client.download_file(filename)
            except IndexError:
                print("Usage: DOWNLOAD filename.py")
        elif command_type == "DELETE":
            try:
                # Extract the filename from the command
                filename = command[7:].strip()
                filename = filename.replace(',', '.')
                client.delete_file(filename)
            except IndexError:
                print("Usage: DELETE filename.py")
        elif command_type == "VIEW":
            # View all files
            client.view_files()
        else:
            # Handle invalid command
            print("Invalid command")

if __name__ == "__main__":
    main()import os
import uuid
from base64 import b64encode, b64decode
from kafka_config import create_producer, create_consumer, FILE_TRANSFER_TOPIC, AUTH_TOPIC
from kafka.errors import KafkaError

class KafkaFileClient:
    """
    A Kafka client for file transfer and authentication.

    :param client_id: Unique client ID
    :param producer: Kafka producer instance
    :param consumer: Kafka consumer instance
    :param username: Username of the authenticated user
    """

    def __init__(self):
        """
        Initialize the client with a unique client ID, create a producer and a consumer instance.
        """
        self.client_id = str(uuid.uuid4())
        self.producer = create_producer()
        self.consumer = create_consumer(f'client_{self.client_id}')
        self.username = None

    def check_server_connection(self):
        """
        Check if the server is reachable by sending a dummy message.
        
        :return: True if the server is reachable, False otherwise
        """
        try:
            self.producer.send(AUTH_TOPIC, {'client_id': self.client_id, 'check': 'ping'})
            self.producer.flush()
            return True
        except KafkaError:
            print("Server is disconnected. Exiting...")
            return False

    def authenticate(self):
        """
        Authenticate the client with a username and password.

        :return: True if authentication is successful, False otherwise
        """
        if not self.check_server_connection():
            return False

        username = input("Enter username: ")
        password = input("Enter password: ")
        
        self.producer.send(AUTH_TOPIC, {
            'client_id': self.client_id,
            'username': username,
            'password': password
        })
        
        try:
            # Subscribe to auth response
            self.consumer.subscribe(['auth_response'])
            for message in self.consumer:
                data = message.value
                if data['client_id'] == self.client_id:
                    if data['success']:
                        # Store the authenticated username
                        self.username = data['username']
                        print("Authentication successful")
                        return True
                    else:
                        print("Authentication failed")
                        return False
        except KafkaError:
            print("Server is disconnected. Exiting...")
            return False  # Terminate the authentication process

    def send_file_transfer_request(self, action, filename=None, content=None):
        """
        Send a file transfer request to the server based on the given action.

        :param action: The action to perform on the server (UPLOAD/DOWNLOAD/DELETE/VIEW)
        :param filename: The filename to use for the action
        :param content: The file content to send for the UPLOAD action
        """
        if not self.check_server_connection():
            return

        request = {
            'client_id': self.client_id,
            'action': action,
            'username': self.username,
            'filename': filename,
            'content': content
        }
        self.producer.send(FILE_TRANSFER_TOPIC, request)

        try:
            # Subscribe to transfer response and wait for the response
            self.consumer.subscribe(['transfer_response'])
            for message in self.consumer:
                response = message.value
                if response['client_id'] == self.client_id:
                    if response['success']:
                        if action == 'UPLOAD':
                            print("Upload complete")
                        elif action == 'DOWNLOAD':
                            print("Download complete")
                        elif action == 'DELETE':
                            print(response['message'])
                        elif action == 'VIEW':
                            files = response['files']
                            if files:
                                print("\nYour files:")
                                for file in files:
                                    print(f"- {file}")
                            else:
                                print("No files found in your storage")
                    else:
                        if action == 'UPLOAD':
                            print(f"Upload failed: {response['message']}")
                        elif action == 'DOWNLOAD':
                            print(f"Download failed: {response['message']}")
                        elif action == 'DELETE':
                            print(f"Delete failed: {response['message']}")
                        elif action == 'VIEW':
                            print(f"View failed: {response['message']}")
                    break
        except KafkaError:
            print("Server is disconnected. Exiting...")
            return  # Terminate the file transfer request

    def upload_file(self, filepath):
        """
        Upload a file from the local storage to the server.

        :param filepath: The path to the file to upload
        """
        # Convert filepath to correct case if file exists with different case
        directory = os.path.dirname(filepath) or '.'
        filename = os.path.basename(filepath)
        
        # Look for the file with case-insensitive match
        actual_filename = None
        for f in os.listdir(directory):
            if f.lower() == filename.lower():
                actual_filename = f
                break
        
        # Use the actual filename if found, otherwise use original filepath
        final_path = os.path.join(directory, actual_filename) if actual_filename else filepath
        
        if not os.path.exists(final_path):
            print(f"Error: File '{filepath}' not found")
            return
        
        try:
            with open(final_path, 'rb') as f:
                content = f.read()
            
            print(f"Reading file: {final_path}")
            print(f"File size: {len(content)} bytes")
            
            filename = os.path.basename(final_path)
            self.send_file_transfer_request('UPLOAD', filename, b64encode(content).decode())
        except Exception as e:
            print(f"Upload error: {str(e)}")
            
    def download_file(self, filename):
        pass
        
    def delete_file(self, filename):
        pass

    def view_files(self):
        pass

def main():
    """
    Main function to run the Kafka file client for file transfer operations.
    It provides an interactive command-line interface for users to
    authenticate and perform file operations such as UPLOAD, DOWNLOAD,
    DELETE, and VIEW.
    """
    client = KafkaFileClient()
    if not client.authenticate():
        return

    while True:
        # Get the command from the user
        command = input("Enter command (UPLOAD/DOWNLOAD/DELETE/VIEW/EXIT): ").strip()
        command_type = command.split()[0].upper()

        if command_type == "EXIT":
            # Exit the program
            break
        elif command_type == "UPLOAD":
            try:
                # Extract the file path from the command
                filepath = command[7:].strip()
                filepath = filepath.replace(',', '.')
                print(f"Attempting to upload: {filepath}")
                client.upload_file(filepath)
            except IndexError:
                print("Usage: UPLOAD filename.py")
        elif command_type == "DOWNLOAD":
            try:
                # Extract the filename from the command
                filename = command[9:].strip()
                filename = filename.replace(',', '.')
                client.download_file(filename)
            except IndexError:
                print("Usage: DOWNLOAD filename.py")
        elif command_type == "DELETE":
            try:
                # Extract the filename from the command
                filename = command[7:].strip()
                filename = filename.replace(',', '.')
                client.delete_file(filename)
            except IndexError:
                print("Usage: DELETE filename.py")
        elif command_type == "VIEW":
            # View all files
            client.view_files()
        else:
            # Handle invalid command
            print("Invalid command")

if __name__ == "__main__":
    main()