from src.db.setup import Setup

if __name__ == "__main__":
    # Set up DB
    print("RUNNING SETUP")
    setup = Setup()
    setup.run()
