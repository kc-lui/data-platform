class BaseDags:
    def __init__(self):
        # DAG ID need to be in uppercase, otherwise it will not show by list_all_dag_ids()
        pass

    def list_all_dag_ids(self):
        return [value for key, value in self.__dict__.items() if key.isupper()]


class ExampleDags(BaseDags):
    def __init__(self):
        super().__init__()
        self.DAG_TRIGGED = "example_cross_dag_relation"
        self.DOCKEROPERATOR_SHOWCASE = "example_lauch_another_service_by_docker_operator"
        self.DYNAMIC_DAG = "example_dynamic_generate_dags"
        self.DYNAMIC_FAN_OUT = "example_dynamic_fan_out"
        self.DYNAMIC_SYMBOL_CHECKER = "example_dynamic_exchange_symbol_checker"
        self.EXAMPLE_CONNECTIONS_TEST_DAG_ID = "example_connections_test"
        self.K8S = "example_k8s"
        self.K8S_POD_OPERATOR = "example_k8s_pod_operator"
        self.SLACK_CALLBACK = "slack_callback"
        self.SLEEP = "sleep_demo"


class MarketDataDags(BaseDags):
    def __init__(self):
        super().__init__()
        self.GRAG_TWSE_SYMBOLS = "grab_twse_symbols"
        self.GRAG_TWSE_OHLCV = "grab_twse_ohlcv"
        self.GENERATE_MARKET_CALENDAR = "generate_market_calendar"
        self.GENERATE_CAPM = "generate_capm"
