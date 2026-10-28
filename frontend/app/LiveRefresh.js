'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';

export default function LiveRefresh() {
  const router = useRouter();

  useEffect(() => {
    const events = new EventSource('/api/events');

    events.onmessage = () => {
      router.refresh();
    };

    return () => {
      events.close();
    };
  }, [router]);

  return null;
}