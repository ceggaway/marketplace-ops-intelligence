import { Suspense, lazy } from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import Layout from './components/Layout'
import Spinner from './components/Spinner'

const Overview     = lazy(() => import('./pages/Overview'))
const ZoneRisk     = lazy(() => import('./pages/ZoneRisk'))
const ActionCenter = lazy(() => import('./pages/ActionCenter'))
const ModelHealth  = lazy(() => import('./pages/ModelHealth'))
const Reports      = lazy(() => import('./pages/Reports'))

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1 } } })

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Layout>
          <Suspense fallback={<Spinner size={36} />}>
            <Routes>
              <Route path="/"           element={<Overview />} />
              <Route path="/zones"      element={<ZoneRisk />} />
              <Route path="/actions"    element={<ActionCenter />} />
              <Route path="/health"     element={<ModelHealth />} />
              <Route path="/reports"    element={<Reports />} />
            </Routes>
          </Suspense>
        </Layout>
      </BrowserRouter>
    </QueryClientProvider>
  )
}
