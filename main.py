from src.app.vi_app import App

if __name__ == "__main__":
    app = App()
    try:
        input_path = "data/input/stock_prices.csv"
        df = app.load_data(input_path)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        app.stop()
