import { SidebarHeader } from './SidebarHeader';
import { SidebarList } from './SidebarList';

export function Sidebar() {
  return (
    <div class="sb">
      <SidebarHeader />
      <div class="sbc" id="sbc">
        <SidebarList />
      </div>
    </div>
  );
}
