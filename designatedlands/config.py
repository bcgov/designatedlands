# Copyright 2017 Province of British Columbia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os

# pick up db_url from $DATABASE_URL if available
if "DATABASE_URL" in os.environ:
    db_url = os.environ["DATABASE_URL"]
else:
    db_url = (
        "postgresql://designatedlands:designatedlands@localhost:5432/designatedlands"
    )

defaultconfig = {
    "dl_path": "source_data",
    "sources_designations": "sources_designations.csv",
    "sources_supporting": "sources_supporting.csv",
    "out_path": "outputs",
    "db_url": db_url,
    "n_processes": -1,
    "resolution": 10,
}
