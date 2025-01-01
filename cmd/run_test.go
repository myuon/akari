package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyze(t *testing.T) {
	writer := bytes.NewBuffer(nil)

	if err := Run(RunOptions{
		ConfigFile: "../akari.init.toml",
		LogFile:    "../testdata/nginx-isucon14.log",
		GlobalSeed: 0,
		Writer:     writer,
	}); err != nil {
		t.Fatalf("failed to analyze: %v", err)
	}

	assert.Equal(t, writer.String(), ` #  Count     Total   Mean    Min    P50    P95    Max    2xx    3xx  4xx  5xx    TotalBs     MeanBs  Method  Url
 1  26823  1151.813  0.043  0.002  0.039  0.089  0.218  26823      0    0    0   759.6 KB    29.0 B     POST  /api/chair/coordinate
 2   8654   434.026  0.050  0.000  0.043  0.116  2.162   8654      0    0    0     2.0 MB   236.0 B      GET  /api/chair/notification
 3   6547   343.511  0.052  0.001  0.048  0.123  0.230   6547      0    0    0     2.4 MB   377.0 B      GET  /api/app/notification
 4    636   171.829  0.270  0.003  0.150  0.856  1.206    636      0    0    0   570.2 KB   918.0 B      GET  /api/app/nearby-chairs?distance=*&latitude=*&longitude=*
 5    440   144.353  0.328  0.034  0.153  1.005  2.583    437      0    0    3    13.0 KB    30.0 B     POST  /api/app/rides/(ulid)/evaluation
 6    934    41.568  0.045  0.003  0.041  0.088  0.186    934      0    0    0     0.0 B      0.0 B     POST  /api/chair/rides/(ulid)/status
 7    639    38.002  0.059  0.000  0.035  0.204  0.455    639      0    0    0   434.4 KB   696.0 B      GET  /api/app/rides
 8    517    36.765  0.071  0.002  0.064  0.173  0.325    517      0    0    0    26.2 KB    51.0 B     POST  /api/app/rides
 9    159    24.550  0.154  0.014  0.127  0.367  0.561    159      0    0    0   230.0 KB     1.4 KB     GET  /api/owner/sales?until=*
10    521    13.807  0.027  0.001  0.023  0.070  0.160    521      0    0    0    14.0 KB    27.0 B     POST  /api/app/rides/estimated-fare
11    190     5.732  0.030  0.001  0.025  0.081  0.122    188      0    2    2    16.0 KB    86.0 B     POST  /api/app/users
12    146     4.363  0.030  0.003  0.026  0.055  0.127    146      0    0    0     0.0 B      0.0 B     POST  /api/chair/activity
13    188     3.792  0.020  0.001  0.017  0.052  0.076    188      0    0    0     0.0 B      0.0 B     POST  /api/app/payment-methods
14    166     3.476  0.021  0.000  0.018  0.060  0.075    166      0    0    0   386.7 KB     2.3 KB     GET  /api/owner/chairs
15    148     3.438  0.023  0.000  0.021  0.062  0.094    148      0    0    0    10.8 KB    75.0 B     POST  /api/chair/chairs
16      1     2.549  2.549  2.549  2.549  2.549  2.549      1      0    0    0    17.0 B     17.0 B     POST  /api/initialize
17  25270     0.519  0.000  0.000  0.000  0.000  0.016   4503  20767    0    0    66.2 MB     2.7 KB     GET  /assets/*
18      1     0.032  0.032  0.032  0.032  0.032  0.032      1      0    0    0     9.0 KB     9.0 KB     GET  /api/owner/sales?=*
19      1     0.028  0.028  0.028  0.028  0.028  0.028      1      0    0    0     8.8 KB     8.8 KB     GET  /api/owner/sales?since=*&until=*
20  11405     0.008  0.000  0.000  0.000  0.000  0.006   1721   9684    0    0    41.2 MB     3.7 KB     GET  /images/*
21      5     0.004  0.001  0.000  0.001  0.001  0.001      5      0    0    0   625.0 B    125.0 B     POST  /api/owner/owners
22    171     0.000  0.000  0.000  0.000  0.000  0.000      6    165    0    0     4.1 KB    24.0 B      GET  /owner
23      1     0.000  0.000  0.000  0.000  0.000  0.000      1      0    0    0   704.0 B    704.0 B      GET  /index.html
24   1437     0.000  0.000  0.000  0.000  0.000  0.000    196   1241    0    0     3.2 MB     2.3 KB     GET  /favicon.ico
25   1437     0.000  0.000  0.000  0.000  0.000  0.000    196   1241    0    0    64.3 KB    45.0 B      GET  /favicon-32x32.png
26      1     0.000  0.000  0.000  0.000  0.000  0.000      1      0    0    0   783.0 B    783.0 B      GET  /favicon-128x128.png
27   1267     0.000  0.000  0.000  0.000  0.000  0.000    191   1076    0    0   131.3 KB   106.0 B      GET  /client
28      1     0.000  0.000  0.000  0.000  0.000  0.000      1      0    0    0  1020.0 B   1020.0 B      GET  /apple-touch-icon-180x180.png
`)
}
