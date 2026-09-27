package workflowtimers

import (
	"errors"
	"fmt"
)

// ErrorKind 把竞态失败分门别类，调用方可用 errors.As 取出 Kind 后分支处理。
type ErrorKind string

const (
	// KindNotFound 定时器不存在（且该请求号也未见过）。
	KindNotFound ErrorKind = "not_found"
	// KindStateConflict 状态机冲突：对已取消/已触发的终态定时器做重排，
	// 或对未处于 claimed 的定时器确认触发等。
	KindStateConflict ErrorKind = "state_conflict"
	// KindVersionConflict 版本冲突：确认触发（或显式带版本的操作）所针对的
	// 版本已不是当前版本——重排已提交，旧版本不得再触发。
	KindVersionConflict ErrorKind = "version_conflict"
	// KindLeaseExpired 租约无效：令牌不属于当前版本（fencing），或租约已过期。
	KindLeaseExpired ErrorKind = "lease_expired"
	// KindIdempotentConflict 幂等冲突：请求号相同，但请求内容与首次不同。
	KindIdempotentConflict ErrorKind = "idempotent_conflict"
	// KindAlreadyExists 定时器号已存在（创建接口）。
	KindAlreadyExists ErrorKind = "already_exists"
)

// Error 是本包所有业务失败携带的结构化错误。
type Error struct {
	Kind    ErrorKind
	TimerID string
	// Version 失败操作携带（或当前记录）的版本，便于调用方记录与重试。
	Version int64
	Message string
}

func (e *Error) Error() string {
	s := string(e.Kind)
	if e.TimerID != "" {
		s += ": timer=" + e.TimerID
	}
	if e.Version != 0 {
		s += fmt.Sprintf(" version=%d", e.Version)
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}

// AsError 尝试把 err 解释成本包的 *Error。
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

func errf(kind ErrorKind, timerID string, version int64, format string, args ...any) error {
	return &Error{
		Kind:    kind,
		TimerID: timerID,
		Version: version,
		Message: fmt.Sprintf(format, args...),
	}
}
