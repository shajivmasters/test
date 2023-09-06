import pysnow
from airflow.hooks.base_hook import BaseHook

class ServiceNowClient:
    def __init__(self, connection_id):
        self.conn_id = connection_id

    def _get_connection(self):
        return BaseHook.get_connection(self.conn_id)

    def _get_credentials(self):
        conn = self._get_connection()
        return {
            'instance': conn.host,
            'user': conn.login,
            'password': conn.get_password(),
        }

    def _get_client(self):
        credentials = self._get_credentials()
        return pysnow.Client(instance=credentials['instance'], user=credentials['user'], password=credentials['password'])

    def query_records(self, table, query=None):
        client = self._get_client()
        query = pysnow.Query(table=table, query=query) if query else pysnow.Query(table=table)
        return query.get().all()

    def insert_record(self, table, data):
        client = self._get_client()
        return client.resource(api_path=f"/table/{table}").create(data)

    def update_record(self, table, sys_id, data):
        client = self._get_client()
        record = client.resource(api_path=f"/table/{table}/{sys_id}")
        return record.update(data)

    def close_record(self, table, sys_id, close_notes):
        data = {'state': '6', 'close_notes': close_notes}
        return self.update_record(table, sys_id, data)

    def create_change_request(self, data):
        return self.insert_record('change_request', data)

    def create_incident(self, data):
        return self.insert_record('incident', data)

    def update_change_request(self, sys_id, data):
        return self.update_record('change_request', sys_id, data)

    def update_incident(self, sys_id, data):
        return self.update_record('incident', sys_id, data)

    def close_change_request(self, sys_id, close_notes):
        return self.close_record('change_request', sys_id, close_notes)

    def close_incident(self, sys_id, close_notes):
        return self.close_record('incident', sys_id, close_notes)


# Instantiate the ServiceNowClient with your ServiceNow Connection ID
service_now_conn_id = "your_servicenow_connection"
sn_client = ServiceNowClient(service_now_conn_id)

# Query incidents
incidents = sn_client.query_records('incident', query={'state': 'active'})

# Close an incident
incident_sys_id = 'your_incident_sys_id'
close_notes = 'Closed via Airflow'
sn_client.close_incident(incident_sys_id, close_notes)

# Create a change request
change_request_data = {'short_description': 'Test Change Request', 'description': 'Testing change request creation'}
sn_client.create_change_request(change_request_data)


import pysnow
from airflow.hooks.base_hook import BaseHook

class ServiceNowClient:
    def __init__(self, connection_id, use_proxy=False, proxy_host=None, proxy_port=None):
        self.conn_id = connection_id
        self.use_proxy = use_proxy
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port

    def _get_connection(self):
        return BaseHook.get_connection(self.conn_id)

    def _get_credentials(self):
        conn = self._get_connection()
        return {
            'instance': conn.host,
            'user': conn.login,
            'password': conn.get_password(),
        }

    def _get_client(self):
        credentials = self._get_credentials()
        
        if self.use_proxy:
            # If a proxy is configured, set it up in the pysnow.Client
            proxies = {
                'http': f'http://{self.proxy_host}:{self.proxy_port}',
                'https': f'https://{self.proxy_host}:{self.proxy_port}'
            }
            return pysnow.Client(instance=credentials['instance'], user=credentials['user'], password=credentials['password'], proxies=proxies)
        else:
            return pysnow.Client(instance=credentials['instance'], user=credentials['user'], password=credentials['password'])

    # ... Other methods as before ...

service_now_conn_id = "your_servicenow_connection"
proxy_client_conn_id = "proxy_connection"  # Use the connection ID you created for your proxy
sn_client = ServiceNowClient(service_now_conn_id, use_proxy=True, proxy_host="your_proxy_host", proxy_port="your_proxy_port")
