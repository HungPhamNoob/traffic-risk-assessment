import { useEffect, useState } from 'react'

function App() {
  const [status, setStatus] = useState('loading...')

  useEffect(() => {
    fetch('http://localhost:8000/health')
      .then(res => res.json())
      .then(data => setStatus(` Backend: ${data.status}`))
      .catch(() => setStatus(' Backend not reachable'))
  }, [])

  return (
    <div style={{ padding: 20, fontFamily: 'sans-serif' }}>
      <h1> Capstone Team 4 Dashboard</h1>
      <p>{status}</p>
      <p><em>Huy sẽ thêm Deck.gl + What-if UI sau</em></p>
    </div>
  )
}

export default App