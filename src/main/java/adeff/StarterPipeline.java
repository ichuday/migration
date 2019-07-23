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
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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

		PCollection<Row> Wknd = weekend_read_1.apply("weekend",SqlTransform.query("SELECT DISTINCT * FROM PCOLLECTION"));

		PCollection<String> financeobj = p
				.apply("financeobj",TextIO.read().from("gs://ad_efficiency_input/Mphasize_Financials/Financials*"));

		final Schema FINANCE_SCHEMA = Schema.builder().addStringField("BeneficiaryFinance").addStringField("CatlibCode")
				.addDecimalField("rNR").addDecimalField("rNCS").addDecimalField("rCtb").addDecimalField("rAC").build();

		PCollection<Row> financeobj_1 = financeobj.apply("financeobj_1",ParDo.of(new DoFn<String, Row>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				if (!c.element().contains("Beneficiary")) {
					String[] strArr2 = c.element().split(",");
					Row appRow = Row
							.withSchema(FINANCE_SCHEMA).addValues(strArr2[0], strArr2[1],strArr2[2],
									Double.valueOf(strArr2[3]), Double.valueOf(strArr2[4]), Double.valueOf(strArr2[5]))
							.build();
					c.output(appRow);
				}
			}
		})).setRowSchema(FINANCE_SCHEMA);

		PCollection<Row> financials = financeobj_1.apply("financials",SqlTransform.query("SELECT DISTINCT * from PCOLLECTION"));
		
//		PCollection<String> gs_output_final36_a = financeobj_1.apply(ParDo.of(new DoFn<Row, String>() {
//		private static final long serialVersionUID = 1L;
//
//		@ProcessElement
//		public void processElement(ProcessContext c) {
//			c.output(c.element().toString());
//			System.out.println(c.element().toString());
//		}
//	}));
//	gs_output_final36_a.apply(TextIO.write().to("gs://adteg2/output/financials"));

		p.run().waitUntilFinish();
	}
}
