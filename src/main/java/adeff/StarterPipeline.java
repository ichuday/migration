/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package adeff;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class StarterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	public static void main(String[] args) {

		PipelineOptionsFactory.register(DataflowPipelineOptions.class);
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataflowPipelineOptions.class);

		options.setNumWorkers(20);
		Pipeline p = Pipeline.create(options);
		
		PCollection<String> weekend_read = p.apply("weekend_read",
				TextIO.read().from("gs://ad_efficiency_input/Period_Mapping/Weekend.csv"));

		final Schema WEEKEND_SCHEMA = Schema.builder().addStringField("EveDate").build();

		PCollection<Row> weekend_read_1 = weekend_read.apply("weekend_read_1", ParDo.of(new DoFn<String, Row>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("EveDate")) {
					String[] vals = c.element().split(",");
					Row appRow = Row.withSchema(WEEKEND_SCHEMA).addValues(vals[0]).build();
					c.output(appRow);
				}
			}
		})).setRowSchema(WEEKEND_SCHEMA);

		PCollection<Row> Wknd = weekend_read_1.apply("weekend",
				SqlTransform.query("SELECT DISTINCT * FROM PCOLLECTION"));

		PCollection<String> financeobj = p.apply("financeobj",
				TextIO.read().from("gs://ad_efficiency_input/Mphasize_Financials/Financials*"));

		Schema FINANCE_SCHEMA = Schema.builder().addStringField("BeneficiaryFinance").addStringField("CatlibCode")
				.addDoubleField("rNR").addDoubleField("rNCS").addDoubleField("rCtb").addDoubleField("rAC").build();
		

		PCollection<Row> financeobj_1 = financeobj.apply("financeobj_1", ParDo.of(new DoFn<String, Row>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Beneficiary")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(FINANCE_SCHEMA).addValues(strArr2[0], strArr2[1], Double.valueOf(strArr2[2]),
									Double.valueOf(strArr2[3]), Double.valueOf(strArr2[4]), Double.valueOf(strArr2[5]))
							.build();
					c.output(appRow);
				}
			}
		})).setRowSchema(FINANCE_SCHEMA);
		

		PCollection<String> curveobj = p.apply("curveobj",
				TextIO.read().from("gs://ad_efficiency_input/Curves/ReachCurves.csv"));

		Schema CURVES_SCHEMA = Schema.builder().addStringField("Brand").addStringField("SBU").addStringField("Division")
				.addDoubleField("Alpha").addDoubleField("Beta").addStringField("Market").addStringField("SubChannel")
				.build();

		PCollection<Row> pojos8 = curveobj.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Alpha")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(CURVES_SCHEMA)
							.addValues(strArr2[0], strArr2[1], strArr2[2], Double.valueOf(strArr2[3].trim()),
									Double.valueOf(strArr2[4].trim()), strArr2[5], strArr2[6])
							.build();
					c.output(appRow);
				}
			}
		})).setRowSchema(CURVES_SCHEMA);

		PCollection<String> ship_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/Shipment/ShipmentInput.csv"));

		Schema SHIPMENT_SCHEMA = Schema.builder().addStringField("Outlet1").addStringField("BrandChapter")
				.addStringField("Beneficiary").addDoubleField("ChannelVolume").addDoubleField("AllOutletVolume")
				.addDoubleField("ProjectionFactor").build();

		PCollection<Row> pojos4 = ship_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Beneficiary")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row
							.withSchema(SHIPMENT_SCHEMA).addValues(strArr2[0], strArr2[1], strArr2[2],
									Double.valueOf(strArr2[3]), Double.valueOf(strArr2[4]), Double.valueOf(strArr2[5]))
							.build();
					c.output(appRow);
				}
			}
		})).setRowSchema(SHIPMENT_SCHEMA);

		PCollection<String> period_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/Period_Mapping/Period.csv"));

		Schema PERIOD_SCHEMA = Schema.builder().addStringField("Source_BDA").addStringField("Start_date")
				.addStringField("End_date").addStringField("Actual_period").build();

		PCollection<Row> pojos6 = period_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Source_BDA")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(PERIOD_SCHEMA).addValues(strArr2[0], strArr2[1], strArr2[2], strArr2[3])
							.build();
					c.output(appRow);
				}
			}
		})).setRowSchema(PERIOD_SCHEMA);

		PCollection<String> basis_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/Basis/Basiscalculation.csv"));

		Schema BASIS_SCHEMA = Schema.builder().addStringField("SourceBDA").addStringField("Brand")
				.addStringField("BasisYearPY").addStringField("BasisYearP2Y").addStringField("BasisYearP3Y").build();

		PCollection<Row> pojos11 = basis_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("SourceBDA")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(BASIS_SCHEMA)
							.addValues(strArr2[0], strArr2[1], strArr2[2], strArr2[3], strArr2[4]).build();
					c.output(appRow);
				}
			}
		})).setRowSchema(BASIS_SCHEMA);

		PCollection<String> composite_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/Composites/composite_file.csv"));

		Schema COMPOSITE_SCHEMA = Schema.builder().addStringField("compositekey").addStringField("subchannel").build();

		PCollection<Row> pojos13 = composite_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("compositekey")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(COMPOSITE_SCHEMA).addValues(strArr2[0], strArr2[1]).build();
					c.output(appRow);
				}
			}
		})).setRowSchema(COMPOSITE_SCHEMA);

		PCollection<String> spend_read = p.apply(TextIO.read().from("gs://ad_efficiency_input/CSVs/Spend/Spend*"));

		Schema SPEND_SCHEMA = Schema.builder().addStringField("FiscalYear").addStringField("FiscalQuarter")
				.addStringField("BrandChapter").addStringField("Market").addStringField("ConsumerBehavior")
				.addStringField("Channel").addStringField("SubChannel").addStringField("Campaign")
				.addStringField("EventName").addStringField("EventKey").addDoubleField("ReportedSpend")
				.addDoubleField("ModeledSpend").build();

		PCollection<Row> spend = spend_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Fiscal Year")) {
					String[] strArr2 = c.element().split(",");
					Double ReportedSpend;
					Double ModeledSpend;
					if (strArr2[12].trim().isEmpty()) {
						ReportedSpend = 0.00;
					} else {
						ReportedSpend = Double.valueOf(strArr2[12].trim());
					}
					if (strArr2[13].trim().isEmpty()) {
						ModeledSpend = 0.00;
					} else {
						ModeledSpend = Double.valueOf(strArr2[13].trim());
					}
					Row appRow = Row.withSchema(SPEND_SCHEMA)
							.addValues(strArr2[0], strArr2[1], strArr2[3], strArr2[5], strArr2[6], strArr2[7],
									strArr2[8], strArr2[9], strArr2[10], strArr2[11], ReportedSpend, ModeledSpend)
							.build();
					c.output(appRow);
				}
			}
		})).setRowSchema(SPEND_SCHEMA);

		PCollection<String> brand_read = p.apply(
				TextIO.read().from("gs://ad_efficiency_input/CSVs/Brand_Media_Hierarchy/Brand_Media_Hierarchy*"));

		Schema BRAND_SCHEMA = Schema.builder().addStringField("Country").addStringField("Division").addStringField("BU")
				.addStringField("Studio").addStringField("Neighborhoods").addStringField("Brand_Chapter")
				.addStringField("Beneficiary").addStringField("Catlib").addStringField("ProdKey").build();

		PCollection<Row> pojos1 = brand_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Country")) {
					String[] strArr2 = c.element().split(",");
					if (strArr2.length > 10) {
						Row appRow = Row.withSchema(BRAND_SCHEMA)
								.addValues(strArr2[1].trim(), strArr2[2].trim(), strArr2[3].trim(), strArr2[4].trim(),
										strArr2[5].trim(), strArr2[6].trim(), strArr2[7].trim(), strArr2[8].trim(),
										strArr2[10].trim())
								.build();
						c.output(appRow);
					}
				}
			}
		})).setRowSchema(BRAND_SCHEMA);

		PCollection<String> event_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/CSVs/Events_csv/MODELED_EVENTS*"));

		Schema EVENT_SCHEMA = Schema.builder().addStringField("EventType").addStringField("EventKey")
				.addStringField("EventName").addStringField("EventComponents").build();

		PCollection<Row> pojos3 = event_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("EvntType")) {
					String[] strArr2 = c.element().split(",");
					for (int j = 0; j < strArr2.length; j++) {
						if (strArr2.length > 3) {
							String[] strsplit = strArr2[3].split("\\|");
							for (int i = 0; i < strsplit.length; i++) {
								Row appRow = Row.withSchema(EVENT_SCHEMA)
										.addValues(strArr2[0], strArr2[1], strArr2[2], strsplit[i]).build();
								c.output(appRow);
							}
						}
					}
				}
			}
		})).setRowSchema(EVENT_SCHEMA);

		PCollection<String> weekly_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/CSVs/Weekly_Dueto/Grocery*"));

		Schema WEEKLY_SCHEMA = Schema.builder().addStringField("Outlet").addStringField("Catlib")
				.addStringField("ProdKey").addStringField("Geogkey").addStringField("Week")
				.addStringField("SalesComponent").addStringField("Dueto_value").addStringField("PrimaryCausalKey")
				.addStringField("Causal_value").addStringField("SourceBDA").build();

		PCollection<Row> pojos = weekly_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Prodkey")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(WEEKLY_SCHEMA).addValues(strArr2[0], strArr2[1], strArr2[2], strArr2[3],
							strArr2[4], strArr2[5], strArr2[6], strArr2[7], strArr2[9], strArr2[10]).build();
					c.output(appRow);
				}
			}
		})).setRowSchema(WEEKLY_SCHEMA);

		PCollection<String> eve_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/CSVs/Events_csv/EVNTEXEC*"));

		Schema EVENTEXEC_SCHEMA = Schema.builder().addStringField("Copy").addStringField("GRPs").addStringField("Start")
				.addStringField("Stop").addStringField("EvntType").addStringField("EvntKey").addStringField("EvntName")
				.build();

		PCollection<Row> pojos111 = eve_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Brand")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(EVENTEXEC_SCHEMA).addValues(strArr2[1], strArr2[2], strArr2[3],
							strArr2[4], strArr2[5], strArr2[6], strArr2[7]).build();
					c.output(appRow);
				}
			}
		})).setRowSchema(EVENTEXEC_SCHEMA);

		PCollection<String> hisp_read = p
				.apply(TextIO.read().from("gs://ad_efficiency_input/Hispanic_Grp/HispanicGrp*"));

		Schema HISPANIC_SCHEMA = Schema.builder().addStringField("Market").addStringField("WeekEnding")
				.addStringField("BrandVariant").addStringField("Creative").addStringField("Brand")
				.addStringField("CommercialDuration").addStringField("Advertisement_id").addStringField("TVHousehold")
				.build();

		PCollection<Row> pojos12 = hisp_read.apply(ParDo.of(new DoFn<String, Row>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Market")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row.withSchema(HISPANIC_SCHEMA).addValues(strArr2[0], strArr2[1], strArr2[2],
							strArr2[3], strArr2[4], strArr2[5], strArr2[6], strArr2[7]).build();
					c.output(appRow);
				}
			}
		})).setRowSchema(HISPANIC_SCHEMA);

//		// Query_0
		PCollection<Row> rec_0 = spend.apply(SqlTransform.query(
				"SELECT FiscalYear, FiscalQuarter, BrandChapter, Market, ConsumerBehavior, Channel, SubChannel, Campaign, EventName, EventKey, ReportedSpend, ModeledSpend, SUBSTRING(FiscalYear FROM 3 FOR 2) as Year1, SUBSTRING(FiscalQuarter FROM 2 FOR 1) as Quarter1 from PCOLLECTION"));

//		// Query_1 Filter
		PCollection<Row> rec_1 = rec_0.apply(SqlTransform.query(
				"SELECT *, ((cast(Year1 as double)*10) + cast(Quarter1 as double)) as Summed, 'a' as Dummy from PCOLLECTION"));

//		// Query_2 Range
		PCollection<Row> rec_2 = rec_1.apply(
				SqlTransform.query("SELECT max(Summed) as maxed, max(Summed)-10 as least, 'a' as Dummy from PCOLLECTION"));

//		// Query_3 Latest_Spend to get one year spend data
		PCollection<Row> rec_3_0 = PCollectionTuple.of(new TupleTag<>("rec_1"), rec_1)
				.and(new TupleTag<>("rec_2"), rec_2).apply(SqlTransform.query(
						"SELECT a.FiscalYear, a.FiscalQuarter, a.BrandChapter, a.Market, a.ConsumerBehavior, a.Channel, a.SubChannel, \r\n"
								+ " a.Campaign, a.EventName, a.EventKey, a.ReportedSpend, a.ModeledSpend, a.Summed, a.Dummy, b.maxed, b.least from rec_1 a INNER JOIN rec_2 b on a.Dummy = b.Dummy "));

		PCollection<Row> rec_3 = rec_3_0.apply(SqlTransform.query("SELECT * from PCOLLECTION where Summed > least and Summed <= maxed "));

		p.run().waitUntilFinish();
	}
}
