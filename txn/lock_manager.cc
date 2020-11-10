// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>

#include "txn/lock_manager.h"

using std::deque;

LockManager::~LockManager() {
  // Cleanup lock_table_
  for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
    delete it->second;
  }
}

deque<LockManager::LockRequest>* LockManager::_getLockQueue(const Key& key) {
  deque<LockRequest> *dq = lock_table_[key];
  if (!dq) {
    dq = new deque<LockRequest>();
    lock_table_[key] = dq;
  }
  return dq;
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  bool empty = true;
  LockRequest rq(EXCLUSIVE, txn);
  deque<LockRequest> *dq = _getLockQueue(key);

  empty = dq->empty();
  dq->push_back(rq);

  if (!empty) { // Add to wait list, doesn't own lock.
    txn_waits_[txn]++;
  }
  return empty;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *queue = _getLockQueue(key);
  bool removedOwner = true; // Is the lock removed the lock owner?

  // Delete the txn's exclusive lock.
  for (auto it = queue->begin(); it < queue->end(); it++) {
    if (it->txn_ == txn) { // TODO is it ok to just compare by address?
        queue->erase(it);
        break;
    }
    removedOwner = false;
  }

  if (!queue->empty() && removedOwner) {
    // Give the next transaction the lock
    LockRequest next = queue->front();

    if (--txn_waits_[next.txn_] == 0) {
        ready_txns_->push_back(next.txn_);
        txn_waits_.erase(next.txn_);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = _getLockQueue(key);
  if (dq->empty()) {
    return UNLOCKED;
  } else {
    vector<Txn*> _owners;
    _owners.push_back(dq->front().txn_);
    *owners = _owners;
    return EXCLUSIVE;
  }
}