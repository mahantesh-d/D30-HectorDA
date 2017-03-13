USE all_trade;
ALTER TABLE stock_adjustment ADD lucene text;
CREATE CUSTOM INDEX ON stock_adjustment (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = { 
	'refresh_seconds': '1',
    'schema': '{
		fields: {
			product_type_key: {type: "text"},
			product_subtype_key: {type: "text"},
			brand_key: {type: "text"},
			mat_code_key: {type: "text"},
			model_key: {type: "text"},
			serial_key: {type: "text"},
			commercial_name_key: {type: "text"},
			adjust_stock_no: {type: "text"},
			location_code: {type: "text"},
			company: {type: "text"},
			adjust_date_time: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"}
			}
		}'
};


ALTER TABLE sub_stock_detail_transfer ADD lucene text;
CREATE CUSTOM INDEX ON sub_stock_detail_transfer (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		to_substock: {type: "text"},
		transfer_substock_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		mat_code_key: {type: "text"},
		transfer_substock_no: {type: "text"}
		}
	}'
};


ALTER TABLE order_transfer ADD lucene text;
CREATE CUSTOM INDEX ON order_transfer (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		from_location_province: {type: "text"},
		from_location_code: {type: "text"},
		product_subtype_key: {type: "text"},
		transfer_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		mat_code_key: {type: "text"},
		transfer_status: {type: "text"},
		from_location_region: {type: "text"},
		transfer_no: {type: "text"},
		update_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		model_key: {type: "text"},
		transaction_type: {type: "text"},
		from_location_subtype: {type: "text"},
		to_location_code: {type: "text"},
		update_by: {type: "text"},
		product_type_key: {type: "text"},
		brand_key: {type: "text"},
		from_location_type: {type: "text"}
		}
	}'
};


ALTER TABLE request_goods ADD lucene text;
CREATE CUSTOM INDEX ON request_goods (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		model_key: {type: "text"},
		product_type_key: {type: "text"},
		brand_key: {type: "text"},
		to_location_subtype: {type: "text"},
		product_subtype_key: {type: "text"},
		to_location_type: {type: "text"},
		request_status: {type: "text"},
		to_location_province: {type: "text"},
		request_no: {type: "text"},
		mat_code_key: {type: "text"},
		transaction_type: {type: "text"},
		update_by: {type: "text"},
		to_location_code: {type: "text"},
		to_location_region: {type: "text"},
		create_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		commercial_name_key: {type: "text"},
		route_group: {type: "text"},
		update_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"}
		}
	}'
};


ALTER TABLE sub_stock_daily_detail ADD lucene text;
CREATE CUSTOM INDEX ON sub_stock_daily_detail (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		sales_for: {type: "text"},
		daily_status: {type: "text"},
		device_payment_name: {type: "text"},
		date_for_sales: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		controller_name: {type: "text"},
		create_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		substock_daily_no: {type: "text"}
		}
	}'
};


ALTER TABLE tranfer_out_mismatch ADD lucene text;
CREATE CUSTOM INDEX ON tranfer_out_mismatch (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		create_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		update_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		data_error_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		create_by: {type: "text"},
		update_by: {type: "text"},
		request_no: {type: "text"}
		}
	}'
};


ALTER TABLE check_stock_detail ADD lucene text;
CREATE CUSTOM INDEX ON check_stock_detail (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		model_key: {type: "text"},
		serial_match_key: {type: "text"},
		confirm_user: {type: "text"},
		update_by: {type: "text"},
		product_subtype_key: {type: "text"},
		stock_on_hand_qty: {type: "text"},
		confirm_no: {type: "text"},
		update_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		stock_on_hand_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		location_code: {type: "text"},
		confirm_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"}
		}
	}'
};


ALTER TABLE sale_out_detail ADD lucene text;
CREATE CUSTOM INDEX ON sale_out_detail (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		update_by: {type: "text"},
		location_code: {type: "text"},
		brand_key: {type: "text"},
		location_region: {type: "text"},
		product_subtype_key: {type: "text"},
		model_key: {type: "text"},
		location_subtype: {type: "text"},
		location_type: {type: "text"},
		commercial_name_key: {type: "text"},
		document_no: {type: "text"},
		update_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"},
		product_type_key: {type: "text"},
		location_province: {type: "text"},
		serial_key: {type: "text"}
		}
	}'
};


ALTER TABLE obtain_detail ADD lucene text;
CREATE CUSTOM INDEX ON obtain_detail (lucene) 
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
	'refresh_seconds': '1',
    'schema': '{
		fields: {
		transaction_type: {type: "text"},
		sales_code_key: {type: "text"},
		obtain_no: {type: "text"},
		location_code: {type: "text"},
		serial_key: {type: "text"},
		obtain_datetime: {type: "date", pattern: "yyyy-MM-dd HH:mm:ss"}
		}
	}'
};
