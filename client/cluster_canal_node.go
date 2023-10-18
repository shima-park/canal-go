package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

/**
修改说明：去掉CanalClusterNode里的ServerRunningData和Event，不再监听，改为随用随取，因为不能确定取到值后就进行链接，会造成数据不一致，
		 如果当前Server挂了，连接会断开，到时候直接重连就可以了
*/

type ServerRunningData struct {
	Cid     int64
	Address string
	Active  bool
}

type CanalClusterNode struct {
	zkClient       *zk.Conn
	destination    string
	clusterAddress []string
}

const (
	cluster_path = "/otter/canal/destinations/%s/cluster"
	running_path = "/otter/canal/destinations/%s/running"
)

func NewCanalClusterNode(destination string, zkServer []string, timeout time.Duration) (*CanalClusterNode, error) {
	zkClient, _, err := zk.Connect(zkServer, timeout)
	if err != nil {
		return nil, fmt.Errorf("zk.Connect: %w", err)
	}

	cluster, _, err := zkClient.Children(fmt.Sprintf(cluster_path, destination))
	if err != nil {
		zkClient.Close()
		return nil, fmt.Errorf("zkClient.ChildrenW err: %w", err)
	}

	rand.Shuffle(len(cluster), func(a, b int) {
		cluster[a], cluster[b] = cluster[b], cluster[a]
	})

	canalNode := &CanalClusterNode{
		zkClient:       zkClient,
		destination:    destination,
		clusterAddress: cluster,
	}

	return canalNode, nil
}

func (canalNode *CanalClusterNode) Close() {
	if canalNode.zkClient != nil {
		canalNode.zkClient.Close()
	}
}

func (canalNode *CanalClusterNode) GetNode() (addr string, port int, err error) {
	if canalNode.zkClient == nil {
		return "", 0, errors.New("zk client is closed")
	}

	serverRunningData, err := canalNode.getRunningServer()
	if err != nil {
		return "", 0, err
	}

	s := strings.Split(serverRunningData.Address, ":")
	if len(s) == 2 && s[0] != "" {
		port, err = strconv.Atoi(s[1])
		if err != nil {
			return "", 0, fmt.Errorf("error canal cluster server %s", serverRunningData.Address)
		}

		addr = s[0]
		return
	} else {
		return "", 0, fmt.Errorf("error canal cluster server %s", serverRunningData.Address)
	}
}

func (canalNode *CanalClusterNode) getRunningServer() (ServerRunningData, error) {
	serverInfo := ServerRunningData{}

	body, _, err := canalNode.zkClient.Get(fmt.Sprintf(running_path, canalNode.destination))
	if err != nil {
		log.Printf("zkClient.GetW err:%v", err)
		return serverInfo, err
	}

	err = json.Unmarshal(body, &serverInfo)
	if err != nil {
		log.Printf("json.Unmarshal err:%v", err)
		return serverInfo, err
	}

	return serverInfo, nil
}
