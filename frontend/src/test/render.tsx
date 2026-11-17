import { render } from '@testing-library/react'
import type { ReactElement } from 'react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { Providers } from '../App'
import { API_KEY } from './server'

export function renderWithApp(ui: ReactElement, { route = '/', path = '*' }: { route?: string; path?: string } = {}) {
  return render(
    <Providers connection={{ baseUrl: 'http://nexus.test', apiKey: API_KEY }}>
      <MemoryRouter initialEntries={[route]}>
        <Routes>
          <Route path={path} element={ui} />
        </Routes>
      </MemoryRouter>
    </Providers>,
  )
}
