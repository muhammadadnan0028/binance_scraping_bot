import websocket
import json
import threading
import csv
from datetime import datetime


class TradeDataProcessor:
    def __init__(self):
        self.previous_price = None
        self.prev_v = None
        self.bigTrade = []
        self.smallTrade = []
        self.price = []
        self.csv_file_name = "trade_data.csv"

        self.intervals = {
            "15 m": 1 * 20,
            "30 m": 2 * 20,
            "1 h": 3 * 20,
            "4 h": 4 * 20,
            "8 h": 5 * 20,
            "12 h": 6 * 20,
            "24 h": 7 * 20,
            "2 d": 8 * 20,
            "3 d": 9 * 20,
            "5 d": 10 * 20,
            "7 d": 11 * 20,
            "10 d": 12 * 20,
            "15 d": 13 * 20,
            "30 d": 14 * 20,
        }

    def format_trade_value(self, value):
        if value >= 1000000:  # 10 Lac = 1 million
            return f"{value/1000000:.2f} M"
        else:
            return f"{value:.2f}"

    def write_to_csv(
        self,
        start_time,
        interval,
        formatted_big_trades,
        formatted_small_trades,
        average_price,
        percentage,
    ):
        with open(self.csv_file_name, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    start_time,
                    interval,
                    formatted_big_trades,
                    formatted_small_trades,
                    f"{average_price:.2f}",
                    f"{percentage:.2f}%",
                ]
            )

    def calculate_and_display(self, interval):
        total_big_trades = sum(self.bigTrade)
        total_small_trades = sum(self.smallTrade)
        formatted_big_trades = self.format_trade_value(total_big_trades)
        formatted_small_trades = self.format_trade_value(total_small_trades)
        average_price = sum(self.price) / len(self.price) if len(self.price) > 0 else 0

        current_price = self.price[-1] if self.price else 0
        period_closing_price = average_price

        if period_closing_price != 0:
            percentage = (
                (current_price - period_closing_price) / period_closing_price
            ) * 100
        else:
            percentage = 0

        start_time = datetime.now().strftime("%m-%d-%Y %H:%M:%S")

        print(
            "{:<25} {:<25} {:<25} {:<25} {:<25} {:<25}".format(
                start_time,
                interval,
                formatted_big_trades,
                formatted_small_trades,
                f"{average_price:.2f}",
                f"{percentage:.2f}%",
            )
        )

        self.write_to_csv(
            start_time,
            interval,
            formatted_big_trades,
            formatted_small_trades,
            average_price,
            percentage,
        )

    def on_message(self, ws, message):
        data = json.loads(message)
        # print(data)
        current_price = float(data["c"])
        current_volume = float(data["v"])
        # print(f"[current_price = {current_price}] [current_volume = {current_volume}]")

        if self.prev_v is None:
            current_volume = 0
        else:
            current_volume = round(abs(self.prev_v - current_volume), 2)

        if self.previous_price is not None and current_price != self.previous_price:
            self.price.append(current_price)
            TradeValue = current_price * current_volume

            if TradeValue >= 100000:
                self.bigTrade.append(TradeValue)
            else:
                self.smallTrade.append(TradeValue)

        self.previous_price = current_price
        self.prev_v = float(data["v"])

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("WebSocket closed")

    def on_open(self, ws):
        symbol = "btcusdt"
        
        ws.send(
            json.dumps({"method": "SUBSCRIBE", "params": [f"{symbol}@ticker"], "id": 1})
        )

    def start_processing(self):
        with open(self.csv_file_name, mode="w", newline="") as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(
                [
                    "Start Time",
                    "period",
                    "BigTrades",
                    "SmallTrades",
                    "Period Closing",
                    "%Price Change",
                ]
            )
        print("\nStart Time:",datetime.now().strftime("%m-%d-%Y %H:%M:%S"),"\n")
        print(
            "{:<25} {:<25} {:<25} {:<25} {:<25} {:<25} ".format(
               "End Time", "period", "BigTrades", "SmallTrades", "Period Closing", "%Price Change"
            )
        )

        websocket.enableTrace(False)
        ws = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.on_open = self.on_open
        
        for interval, seconds in self.intervals.items():
            timer = threading.Timer(
                seconds, self.calculate_and_display, args=(interval,)
            )
            timer.start()

        ws.run_forever()


if __name__ == "__main__":
    processor = TradeDataProcessor()
    processor.start_processing()