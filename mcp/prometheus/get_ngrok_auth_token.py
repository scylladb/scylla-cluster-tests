from sdcm.keystore import KeyStore


if __name__ == "__main__":
    print(KeyStore().get_ngrok_auth_token().decode().strip())
