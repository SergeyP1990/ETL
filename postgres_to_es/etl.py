from config import Config

if __name__ == "__main__":
    conf = Config.parse_config("./config")
    print(conf)
