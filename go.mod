module highvolume.io/shackle

go 1.13

replace github.com/lni/dragonboat/v3 => ../dragonboat

require (
	github.com/benbjohnson/clock v1.0.2
	github.com/bmatsuo/lmdb-go v1.8.0
	github.com/fasthttp/router v1.1.6
	github.com/golang/protobuf v1.4.1
	github.com/google/uuid v1.1.1 // indirect
	github.com/kevburnsjr/tci-lru v0.0.0-20190725165011-4be840d4dd55
	github.com/lni/dragonboat/v3 v3.1.1-0.20200703035341-ab2240ccadf9
	github.com/onrik/logrus v0.5.1
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/common v0.10.0
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/segmentio/encoding v0.1.14 // indirect
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.4.0
	github.com/tidwall/gjson v1.6.0 // indirect
	github.com/tsenart/vegeta/v12 v12.8.3 // indirect
	github.com/valyala/fasthttp v1.14.0
	github.com/valyala/fastjson v1.5.1
	golang.org/x/sys v0.0.0-20200615200032-f1bc736245b1 // indirect
	google.golang.org/grpc v1.27.0
	google.golang.org/protobuf v1.24.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200601152816-913338de1bd2
)
