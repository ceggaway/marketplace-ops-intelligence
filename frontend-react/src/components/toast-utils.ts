export interface ToastMessage {
  id: number
  message: string
  type?: 'info' | 'success' | 'warning' | 'error'
}

let _nextId = 1
export const toastListeners: Set<(t: ToastMessage) => void> = new Set()

export function showToast(message: string, type: ToastMessage['type'] = 'info') {
  const toast: ToastMessage = { id: _nextId++, message, type }
  toastListeners.forEach(fn => fn(toast))
}
