package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

var IsEtcd bool = false

type etcdCoordinator struct {
	nodeIp       string
	nodePort     string
	nodeType     string
	globalLogger logr.Logger
	client       *clientv3.Client
	kvc          clientv3.KV
	lease        *clientv3.LeaseGrantResponse
	mu           sync.Mutex
}

var etcdObj etcdCoordinator

func InitEtcd(eaddr string, ipaddr string, port string, ntype string, logger logr.Logger) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{eaddr},
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error(err, "unable to connect to etcd")
		return
	}
	logger.Info("etcd client connected", "eaddr", eaddr, "ipaddr", ipaddr, "port", port, "ntype", ntype)
	IsEtcd = true
	kvc := clientv3.NewKV(cli)
	etcdObj = etcdCoordinator{
		client:       cli,
		nodeIp:       ipaddr,
		nodePort:     port,
		nodeType:     ntype,
		globalLogger: logger,
		kvc:          kvc,
	}

	createHostLease()

	ticker := time.NewTicker(1 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				notifyAlive()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()

}

type Load struct {
	Cpu  float64 `json:"cpu"`
	Mem  float64 `json:"mem"`
	Ip   string  `json:"ip"`
	Port string  `json:"port"`
}

func getHostLoad() Load {
	v, _ := mem.VirtualMemory()
	x, _ := cpu.Percent(time.Second, false)
	load := Load{
		Cpu:  x[0],
		Mem:  v.UsedPercent,
		Ip:   etcdObj.nodeIp,
		Port: etcdObj.nodePort,
	}
	return load
}

func createHostLease() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	// First lets create a lease for the host
	lease, err := etcdObj.client.Grant(ctx, 10) //10sec
	if err != nil {
		etcdObj.globalLogger.Error(err, "error acquiring lease for session key")
		return
	}
	etcdObj.lease = lease
	etcdObj.globalLogger.Info("Got lease", "ID", lease.ID, "TTL", lease.TTL)
	load := getHostLoad()
	b, _ := json.Marshal(load)
	etcdObj.kvc.Put(ctx, "available-hosts/"+getHostKey(), string(b), clientv3.WithLease(lease.ID))
}

func notifyAlive() {
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	if etcdObj.lease != nil {
		leaseKeepAlive, err := etcdObj.client.KeepAlive(context.Background(), etcdObj.lease.ID)
		if err != nil {
			etcdObj.globalLogger.Error(err, "error activating keepAlive for lease", "leaseID", etcdObj.lease.ID)
		}

		// === === see here
		go func() {
			for {
				<-leaseKeepAlive
			}
		}()
		load := getHostLoad()
		b, _ := json.Marshal(load)
		etcdObj.kvc.Put(context.Background(), "available-hosts/"+getHostKey(), string(b), clientv3.WithLease(etcdObj.lease.ID))
		// etcdObj.globalLogger.Info("Host Alive", "leaseKeepAlive", <-leaseKeepAlive)
	}
}

func Close() {
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	if IsEtcd {
		etcdObj.client.Close()
	}
}

func getHostKey() string {
	value := etcdObj.nodeIp + ":" + etcdObj.nodePort // + ":" + etcdObj.nodeType
	return strings.Replace(value, "::", ":", -1)
}

func RegisterSession(session string) {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	value := getHostKey()
	// key := fmt.Sprintf("/session/%v", session)
	// etcdObj.globalLogger.Info("Regsiter Session:", "key", key, "Value", value)
	// resp, _ := kvc.Put(ctx, key, value)
	// rev := resp.Header.Revision
	// etcdObj.globalLogger.Info("Register Session:", "rev", rev)
	key2 := fmt.Sprintf("/session/%v/node/%v", session, value)
	kvc.Put(ctx, key2, "")

	cancel()
}
func CloseSession(session string) {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	key := fmt.Sprintf("/session/%v", session)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, _ := kvc.Delete(ctx, key)
	etcdObj.globalLogger.Info("Deleted", "count", resp.Deleted)
	value := getHostKey()
	key2 := fmt.Sprintf("/session/%v/node/%v", session, value)
	kvc.Delete(ctx, key2)
	cancel()
}

var sessionlease map[string]clientv3.LeaseGrantResponse

func CurrentSessionPeer(stats map[string][]string) {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	// First lets create a lease for the host
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	// lease, err := etcdObj.client.Grant(ctx, 10) //10sec
	// if err != nil {
	// 	etcdObj.globalLogger.Error(err, "error acquiring lease for session key")
	// 	return
	// }
	value := getHostKey()
	key := fmt.Sprintf("/current_session_map/node/%v", value)
	b, _ := json.Marshal(stats)
	// etcdObj.globalLogger.Info("string(b) %v", string(b))
	// kvc.Put(ctx, key, string(b), clientv3.WithLease(lease.ID))
	kvc.Put(ctx, key, string(b)) //lease not needed as just watching for new PUT not old data
	cancel()
}

func RegisterSessionPeer(session string, peerid string) {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	value := getHostKey()
	key := fmt.Sprintf("/session/%v/node/%v/peer/%v", session, value, peerid)
	etcdObj.globalLogger.Info("key add", "key", key)
	kvc.Put(ctx, key, "")
	cancel()
}

func CloseSessionPeer(session string, peerid string) {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	value := getHostKey()
	key := fmt.Sprintf("/session/%v/node/%v/peer/%v", session, value, peerid)
	etcdObj.globalLogger.Info("key delete", "key", key)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	kvc.Delete(ctx, key)
	cancel()
}

func RegisterSessionPeerTrack(session string, peerid string, trackid string, trackType string) {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	value := getHostKey()
	key := fmt.Sprintf("/session/%v/node/%v/peer/%v/track/%v/type/%v", session, value, peerid, trackid, trackType)
	etcdObj.globalLogger.Info("key add", "key", key)
	kvc.Put(ctx, key, "")
	cancel()
}

func CloseSessionPeerTrack(session string, peerid string, trackid string, trackType string) {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	value := getHostKey()
	key := fmt.Sprintf("/session/%v/node/%v/peer/%v/track/%v/type/%v", session, value, peerid, trackid, trackType)
	etcdObj.globalLogger.Info("key delete", "key", key)
	kvc.Delete(ctx, key)
	cancel()
}

func TestKV() {
	if !IsEtcd {
		return
	}
	etcdObj.mu.Lock()
	defer etcdObj.mu.Unlock()
	kvc := etcdObj.kvc
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := kvc.Put(ctx, "sample_key", "sample_value")

	rev := resp.Header.Revision
	fmt.Println("Revision:", rev)

	if err != nil {
		panic(err)
	}
	fmt.Println(resp)

	gr, _ := kvc.Get(ctx, "sample_key")
	fmt.Println("Value: ", string(gr.Kvs[0].Value), "Revision: ", gr.Header.Revision)

	dresp, _ := kvc.Delete(ctx, "sample_key")
	fmt.Println("Deleted", dresp.Deleted)
	cancel()
}
