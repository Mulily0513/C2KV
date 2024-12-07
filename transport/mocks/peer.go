// Code generated by MockGen. DO NOT EDIT.
// Source: ./peer.go

// Package mock is a generated GoMock package.
package mock

import (
	pb "github.com/Mulily0513/C2KV/pb"
	gomock "github.com/golang/mock/gomock"
	io "io"
	reflect "reflect"
	time "time"
)

// MockPeer is a mock of Peer interface.
type MockPeer struct {
	ctrl     *gomock.Controller
	recorder *MockPeerMockRecorder
}

// MockPeerMockRecorder is the mock recorder for MockPeer.
type MockPeerMockRecorder struct {
	mock *MockPeer
}

// NewMockPeer creates a new mock instance.
func NewMockPeer(ctrl *gomock.Controller) *MockPeer {
	mock := &MockPeer{ctrl: ctrl}
	mock.recorder = &MockPeerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPeer) EXPECT() *MockPeerMockRecorder {
	return m.recorder
}

// ActiveSince mocks base method.
func (m *MockPeer) ActiveSince() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ActiveSince")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// ActiveSince indicates an expected call of ActiveSince.
func (mr *MockPeerMockRecorder) ActiveSince() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ActiveSince", reflect.TypeOf((*MockPeer)(nil).ActiveSince))
}

// AttachConn mocks base method.
func (m *MockPeer) AttachConn(conn io.WriteCloser) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AttachConn", conn)
}

// AttachConn indicates an expected call of AttachConn.
func (mr *MockPeerMockRecorder) AttachConn(conn interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AttachConn", reflect.TypeOf((*MockPeer)(nil).AttachConn), conn)
}

// Send mocks base method.
func (m_2 *MockPeer) Send(m pb.Message) {
	m_2.ctrl.T.Helper()
	m_2.ctrl.Call(m_2, "Send", m)
}

// Send indicates an expected call of Send.
func (mr *MockPeerMockRecorder) Send(m interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockPeer)(nil).Send), m)
}

// Stop mocks base method.
func (m *MockPeer) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockPeerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockPeer)(nil).Stop))
}